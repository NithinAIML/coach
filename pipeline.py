#!/usr/bin/env python3
import os
import re
import io
import json
import time
import hashlib
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional, Iterable

import boto3
import requests
import openai
from tqdm import tqdm
from bs4 import BeautifulSoup

# vector + chunking
from pinecone import Pinecone, ServerlessSpec
try:
    from pinecone.exceptions import PineconeApiException  # type: ignore
except Exception:
    try:
        from pinecone.exceptions.exceptions import PineconeApiException  # type: ignore
    except Exception:
        class PineconeApiException(Exception):
            pass

from langchain.text_splitter import RecursiveCharacterTextSplitter

# ------- File readers -------
import fitz  # PyMuPDF
from pdfminer.high_level import extract_text as pdfminer_extract_text
from docx import Document as DocxDocument
from pptx import Presentation
import pandas as pd

# =========================
# Defaults / placeholders
# =========================
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Put your ARNs (or pass via CLI flags)
PINECONE_SECRET_ARN = "<PINECONE_SECRET_ARN>"  # {"apiKey":"..."}
OPENAI_SECRET_ARN   = "<AZURE_OPENAI_SP_SECRET_ARN>"  # {"AzureServicePrincipalId":"...", "Password":"..."}

# Azure OpenAI settings (override via CLI if you want)
AZURE_TENANT      = "<TENANT_ID_OR_NAME>"
AZURE_API_BASE    = "https://<your-azure-openai-resource>.openai.azure.com/"
AZURE_API_VERSION = "2023-05-15"
EMBED_DEPLOYMENT  = "text-embedding-ada-002"  # your embedding deployment name

# =========================
# Secrets + clients
# =========================
def pinecone_config(secret_arn: str) -> Pinecone:
    sm = boto3.client("secretsmanager", region_name=AWS_REGION)
    resp = sm.get_secret_value(SecretId=secret_arn)
    obj = json.loads(resp["SecretString"])
    api_key = str(obj["apiKey"]).replace('"', "")
    return Pinecone(api_key=api_key)

def openai_api_config(secret_arn: str, tenant_name: str, api_base: str, api_version: str) -> str:
    """Configure OpenAI SDK to use Azure AD bearer token from a Service Principal."""
    from msal import ConfidentialClientApplication

    sm = boto3.client("secretsmanager", region_name=AWS_REGION)
    resp = sm.get_secret_value(SecretId=secret_arn)
    obj = json.loads(resp["SecretString"])

    client_id = obj["AzureServicePrincipalId"]
    client_secret = str(obj["Password"]).replace('"', "")

    authority = f"https://login.microsoftonline.com/{tenant_name}"
    scopes = ["https://cognitiveservices.azure.com/.default"]

    app = ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority,
    )
    result = app.acquire_token_for_client(scopes=scopes)
    if "access_token" not in result:
        raise RuntimeError("Unable to obtain Azure AD access token for OpenAI")
    token = result["access_token"]

    openai.api_type = "azure_ad"
    openai.api_key = token
    openai.api_base = api_base.rstrip("/") + "/"
    openai.api_version = api_version
    return EMBED_DEPLOYMENT

# =========================
# Pinecone helpers
# =========================
def ensure_index_and_get_handle(
    pc: Pinecone,
    index_name: str,
    *,
    dimension: int = 1536,
    metric: str = "cosine",
    cloud: str = "aws",
    region: str = "us-east-1",
):
    """Idempotently create (or reuse) the index, then bind to its private host."""
    created = False
    try:
        pc.describe_index(index_name)
    except Exception:
        try:
            pc.create_index(
                name=index_name,
                dimension=dimension,
                metric=metric,
                spec=ServerlessSpec(cloud=cloud, region=region),
                deletion_protection="disabled",
            )
            created = True
        except Exception as e:
            if "ALREADY" not in str(e).upper() and "409" not in str(e):
                raise

    # wait until ready if SDK exposes it; otherwise short sleep
    for _ in range(60):
        desc = pc.describe_index(index_name)
        ready = desc.get("status", {}).get("ready", True)
        if ready:
            break
        time.sleep(2)

    host = pc.describe_index(index_name)["host"]
    parts = host.split(".")
    private_host = f"{'.'.join(parts[:2])}.private.{'.'.join(parts[2:])}"
    index = pc.Index(host=private_host)
    return index, created

# =========================
# Confluence helpers
# =========================
def get_base_url(url: str) -> str:
    # works for https://<sub>.atlassian.net/wiki/spaces/KEY/... or /pages/ID/...
    m = re.match(r"^(https?://[^/]+/wiki)", url)
    if m:
        return m.group(1)
    m2 = re.match(r"^(https?://[^/]+)(?=/spaces)", url)
    if m2:
        return m2.group(1)
    raise ValueError(f"Cannot derive base Confluence URL from: {url}")

def get_page_id(url: str) -> str:
    m = re.search(r"pages/(\d+)", url)
    if m:
        return m.group(1)
    raise ValueError(f"No page ID found in URL: {url}")

def confluence_descendants(base_url: str, page_id: str, auth: Tuple[str, str],
                           max_pages: int = 150, max_depth: int = 3) -> List[str]:
    """
    Use Confluence REST to pull descendant pages (IDs) up to max_pages / max_depth.
    Returns list of page IDs (strings), including the root.
    """
    session = requests.Session()
    session.auth = auth
    headers = {"Accept": "application/json"}
    ids = [page_id]
    try:
        # Confluence has an endpoint for descendants; single call is often enough
        url = f"{base_url}/rest/api/content/{page_id}/descendant/page?limit=200"
        r = session.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        data = r.json()
        for it in data.get("results", []):
            pid = str(it.get("id"))
            if pid and pid not in ids:
                ids.append(pid)
                if len(ids) >= max_pages:
                    break
    except Exception as e:
        print(f"[WARN] descendant crawl failed for {page_id}: {e}")
    return ids[:max_pages]

def confluence_fetch_pages(urls: List[str], username: str, api_token: str,
                           include_children: bool = False,
                           max_pages: int = 150, max_depth: int = 3) -> Dict[str, str]:
    """
    For each page URL:
      - if include_children=True treat as 'root': fetch descendants + the page
      - else treat as single page
    Returns {url: text}
    """
    out: Dict[str, str] = {}
    auth = (username, api_token)

    for u in urls:
        try:
            base = get_base_url(u)
            pid = get_page_id(u)
        except Exception as e:
            print(f"[WARN] skip non-page URL: {u} ({e})")
            continue

        page_ids = [pid]
        if include_children:
            page_ids = confluence_descendants(base, pid, auth, max_pages=max_pages, max_depth=max_depth)

        # Load page contents via Confluence REST (clean body storage)
        # We prefer REST over LangChain loader for speed and fewer deps here.
        sess = requests.Session()
        sess.auth = auth
        hdr = {"Accept": "application/json"}
        for p in page_ids:
            try:
                api = f"{base}/rest/api/content/{p}?expand=body.storage,version,ancestors"
                r = sess.get(api, headers=hdr, timeout=30)
                r.raise_for_status()
                data = r.json()
                title = data.get("title", "")
                html = data.get("body", {}).get("storage", {}).get("value", "")
                text = html_to_text(html)
                text = f"{title}\n\n{text}".strip()
                page_url = f"{base}/pages/{p}"
                out[page_url] = text
            except Exception as e:
                print(f"[WARN] fetch fail for page {p}: {e}")
    return out

def html_to_text(html: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    # remove scripts/styles
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    # collapse multiple blank lines
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    return text.strip()

# =========================
# Coveo helpers (optional)
# =========================
class CoveoClient:
    def __init__(self, org_id: str, platform_token: str):
        self.org_id = org_id
        self.platform_token = platform_token
        self.platform_base = "https://platform.cloud.coveo.com/rest/search/v2"
        self.search_url = f"https://{org_id}.org.coveo.com/rest/search/v2"

    def get_user_token(self, user_email: str) -> str:
        url = f"{self.platform_base}/token"
        payload = {
            "organizationId": self.org_id,
            "validFor": 1800000,
            "userIds": [{"name": user_email, "provider": "Email Security Provider"}],
        }
        headers = {
            "authorization": f"Bearer {self.platform_token}",
            "content-type": "application/json",
        }
        r = requests.post(url, json=payload, headers=headers, timeout=30)
        r.raise_for_status()
        return r.json().get("token", "")

    def search_by_label(self, label: str, user_token: str) -> List[str]:
        headers = {
            "authorization": f"Bearer {user_token}",
            "content-type": "application/json",
        }
        params = {"organizationId": self.org_id}
        payload = {"q": f"@conflabels={label}"}
        r = requests.post(self.search_url, params=params, json=payload, headers=headers, timeout=30)
        r.raise_for_status()
        data = r.json()
        urls = [res.get("clickUri") for res in data.get("results", []) if res.get("clickUri")]
        return urls

# =========================
# File extraction
# =========================
@dataclass
class SourceDoc:
    source_id: str        # url or file path (unique id)
    source_type: str      # 'confluence' | 'file'
    page: Optional[int]   # for PDFs; None otherwise
    text: str

def extract_from_pdf(path: str) -> List[SourceDoc]:
    docs: List[SourceDoc] = []
    try:
        # Primary: PyMuPDF page-wise
        with fitz.open(path) as pdf:
            for i, page in enumerate(pdf, start=1):
                txt = page.get_text("text")
                if not txt.strip():
                    continue
                docs.append(SourceDoc(source_id=f"{path}#page={i}", source_type="file", page=i, text=txt))
    except Exception as e:
        print(f"[WARN] PyMuPDF failed for {path}: {e}. Falling back to pdfminer.")
        try:
            txt_all = pdfminer_extract_text(path) or ""
            if txt_all.strip():
                docs.append(SourceDoc(source_id=path, source_type="file", page=None, text=txt_all))
        except Exception as e2:
            print(f"[WARN] pdfminer also failed for {path}: {e2}")
    return docs

def extract_from_docx(path: str) -> List[SourceDoc]:
    d = DocxDocument(path)
    paras = [p.text for p in d.paragraphs]
    txt = "\n".join([t for t in paras if t is not None]).strip()
    return [SourceDoc(source_id=path, source_type="file", page=None, text=txt)] if txt else []

def extract_from_pptx(path: str) -> List[SourceDoc]:
    prs = Presentation(path)
    out: List[SourceDoc] = []
    for i, slide in enumerate(prs.slides, start=1):
        texts = []
        for shape in slide.shapes:
            if hasattr(shape, "text"):
                texts.append(shape.text)
        txt = "\n".join(t for t in texts if t).strip()
        if txt:
            out.append(SourceDoc(source_id=f"{path}#slide={i}", source_type="file", page=i, text=txt))
    return out

def extract_from_csv(path: str) -> List[SourceDoc]:
    try:
        df = pd.read_csv(path)
        txt = df.to_csv(index=False)
        return [SourceDoc(source_id=path, source_type="file", page=None, text=txt)]
    except Exception:
        # try semicolon or tab
        for sep in [";", "\t", "|"]:
            try:
                df = pd.read_csv(path, sep=sep)
                return [SourceDoc(source_id=path, source_type="file", page=None, text=df.to_csv(index=False))]
            except Exception:
                continue
    return []

def extract_from_xlsx(path: str) -> List[SourceDoc]:
    out: List[SourceDoc] = []
    xls = pd.ExcelFile(path)
    for sheet in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sheet)
        txt = df.to_csv(index=False)
        out.append(SourceDoc(source_id=f"{path}#sheet={sheet}", source_type="file", page=None, text=txt))
    return out

def extract_from_json(path: str) -> List[SourceDoc]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    txt = json.dumps(data, indent=2, ensure_ascii=False)
    return [SourceDoc(source_id=path, source_type="file", page=None, text=txt)]

def extract_from_html(path: str) -> List[SourceDoc]:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        html = f.read()
    txt = html_to_text(html)
    return [SourceDoc(source_id=path, source_type="file", page=None, text=txt)] if txt else []

def extract_plain_text(path: str) -> List[SourceDoc]:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        txt = f.read()
    return [SourceDoc(source_id=path, source_type="file", page=None, text=txt)] if txt.strip() else []

def extract_files(paths: List[str]) -> List[SourceDoc]:
    out: List[SourceDoc] = []
    for p in paths:
        ext = os.path.splitext(p)[1].lower()
        try:
            if ext in [".pdf"]:
                out.extend(extract_from_pdf(p))
            elif ext in [".docx"]:
                out.extend(extract_from_docx(p))
            elif ext in [".pptx"]:
                out.extend(extract_from_pptx(p))
            elif ext in [".csv"]:
                out.extend(extract_from_csv(p))
            elif ext in [".xlsx", ".xls"]:
                out.extend(extract_from_xlsx(p))
            elif ext in [".json"]:
                out.extend(extract_from_json(p))
            elif ext in [".html", ".htm"]:
                out.extend(extract_from_html(p))
            elif ext in [".md", ".txt", ".log"]:
                out.extend(extract_plain_text(p))
            else:
                print(f"[WARN] Unknown extension {ext} for {p}, reading as text")
                out.extend(extract_plain_text(p))
        except Exception as e:
            print(f"[WARN] file extraction failed for {p}: {e}")
    return out

# =========================
# Chunking + embeddings
# =========================
def md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8"), usedforsecurity=False).hexdigest()

def choose_chunk_params(src: SourceDoc) -> Tuple[int, int]:
    # Heuristics: slightly larger chunks for PDFs/Slides
    if src.page is not None or "#slide=" in src.source_id:
        return 1000, 150
    if src.source_type == "confluence":
        return 800, 120
    return 700, 100

def chunk_sources(sources: List[SourceDoc]) -> List[Dict]:
    chunks: List[Dict] = []
    for s in sources:
        csize, cover = choose_chunk_params(s)
        splitter = RecursiveCharacterTextSplitter(chunk_size=csize, chunk_overlap=cover)
        parts = splitter.split_text(s.text or "")
        for i, txt in enumerate(parts):
            cid = f"{md5(s.source_id)}_{i}"
            meta = {
                "source": s.source_id,
                "source_type": s.source_type,
            }
            if s.page is not None:
                meta["page"] = s.page
            chunks.append({"id": cid, "text": txt, "metadata": meta})
    return chunks

def embed_batch(texts: List[str], deployment: str) -> List[List[float]]:
    # Simple retry with backoff
    wait = 2
    for attempt in range(4):
        try:
            resp = openai.Embedding.create(input=texts, engine=deployment)
            return [d["embedding"] for d in resp["data"]]
        except Exception as e:
            if attempt == 3:
                raise
            time.sleep(wait)
            wait *= 2
    return []

def upsert_chunks(index, chunks: List[Dict], deployment: str, batch_size: int = 32) -> int:
    total = 0
    for i in tqdm(range(0, len(chunks), batch_size), desc="Upserting"):
        batch = chunks[i:i + batch_size]
        embs = embed_batch([c["text"] for c in batch], deployment)
        if not embs:
            continue
        vecs = [(c["id"], e, c["metadata"]) for c, e in zip(batch, embs)]
        index.upsert(vecs)
        total += len(vecs)
    return total

# =========================
# Reporting
# =========================
@dataclass
class StatRow:
    source: str
    source_type: str
    words: int
    chunks: int

def build_report(sources: List[SourceDoc], chunks: List[Dict]) -> Dict:
    # words per source
    by_src: Dict[str, StatRow] = {}
    for s in sources:
        words = len((s.text or "").split())
        if s.source_id not in by_src:
            by_src[s.source_id] = StatRow(s.source_id, s.source_type, 0, 0)
        by_src[s.source_id].words += words
    # chunk counts
    for c in chunks:
        src = c["metadata"].get("source")
        if src and src in by_src:
            by_src[src].chunks += 1
    total_words = sum(r.words for r in by_src.values())
    total_chunks = sum(r.chunks for r in by_src.values())
    return {
        "sources": [r.__dict__ for r in by_src.values()],
        "totals": {"words": total_words, "chunks": total_chunks},
    }

# =========================
# Orchestrator
# =========================
def run_pipeline(
    *,
    index_name: str,
    # secrets / clients
    pinecone_secret_arn: str = PINECONE_SECRET_ARN,
    openai_secret_arn: str = OPENAI_SECRET_ARN,
    tenant: str = AZURE_TENANT,
    api_base: str = AZURE_API_BASE,
    api_version: str = AZURE_API_VERSION,

    # confluence
    confluence_pages: List[str],
    confluence_roots: List[str],
    confluence_username: Optional[str],
    confluence_api_token: Optional[str],
    max_pages: int = 150,
    max_depth: int = 3,

    # coveo (optional)
    coveo_org_id: Optional[str] = None,
    coveo_platform_token: Optional[str] = None,
    coveo_user_email: Optional[str] = None,
    coveo_labels: Optional[List[str]] = None,

    # files
    files: List[str],

    # output
    report_path: Optional[str] = None,
):
    # 1) Configure OpenAI + Pinecone
    deployment = openai_api_config(openai_secret_arn, tenant, api_base, api_version)
    pc = pinecone_config(pinecone_secret_arn)
    index, created = ensure_index_and_get_handle(pc, index_name, dimension=1536, metric="cosine", cloud="aws", region=AWS_REGION)
    print("Created index" if created else "Using existing index", index_name)

    # 2) Collect sources
    collected: List[SourceDoc] = []

    # 2a) Confluence via explicit page URLs
    if confluence_pages or confluence_roots:
        if not (confluence_username and confluence_api_token):
            raise RuntimeError("Confluence username/api token required for Confluence intake.")

        # pages
        if confluence_pages:
            data = confluence_fetch_pages(confluence_pages, confluence_username, confluence_api_token, include_children=False)
            for url, text in data.items():
                collected.append(SourceDoc(source_id=url, source_type="confluence", page=None, text=text))

        # roots (crawl descendants)
        if confluence_roots:
            data = confluence_fetch_pages(confluence_roots, confluence_username, confluence_api_token,
                                          include_children=True, max_pages=max_pages, max_depth=max_depth)
            for url, text in data.items():
                collected.append(SourceDoc(source_id=url, source_type="confluence", page=None, text=text))

    # 2b) Coveo expansion (optional: by label/tag)
    if coveo_org_id and coveo_platform_token and coveo_user_email and coveo_labels:
        try:
            cc = CoveoClient(coveo_org_id, coveo_platform_token)
            user_token = cc.get_user_token(coveo_user_email)
            urls: List[str] = []
            for label in coveo_labels:
                urls.extend(cc.search_by_label(label, user_token))
            urls = list(dict.fromkeys(urls))
            if urls:
                if not (confluence_username and confluence_api_token):
                    print("[WARN] Coveo URLs returned but no Confluence credentials provided; skipping Confluence fetch.")
                else:
                    data = confluence_fetch_pages(urls, confluence_username, confluence_api_token, include_children=False)
                    for url, text in data.items():
                        collected.append(SourceDoc(source_id=url, source_type="confluence", page=None, text=text))
        except Exception as e:
            print(f"[WARN] Coveo expansion failed: {e}")

    # 2c) Files
    if files:
        collected.extend(extract_files(files))

    if not collected:
        print("No sources collected. Nothing to do.")
        return

    # 3) Chunking
    chunks = chunk_sources(collected)

    # 4) Embeddings + upsert
    total_vecs = upsert_chunks(index, chunks, deployment)

    # 5) Report
    report = build_report(collected, chunks)
    report["pinecone"] = {"index": index_name, "vectors_written": total_vecs}
    if report_path:
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
    print(json.dumps(report["totals"], indent=2))
    print(f"Vectors written: {total_vecs}")

# =========================
# CLI
# =========================
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Confluence/Coveo + Files -> Chunk -> Embed (Azure OpenAI) -> Pinecone")
    p.add_argument("--index", required=True, help="Pinecone index name (reused if exists, created otherwise)")

    # secrets + azure
    p.add_argument("--pinecone-secret-arn", default=PINECONE_SECRET_ARN)
    p.add_argument("--openai-secret-arn", default=OPENAI_SECRET_ARN)
    p.add_argument("--tenant", default=AZURE_TENANT)
    p.add_argument("--api-base", default=AZURE_API_BASE)
    p.add_argument("--api-version", default=AZURE_API_VERSION)

    # confluence
    p.add_argument("--confluence-username")
    p.add_argument("--confluence-api-token")
    p.add_argument("--confluence-page", action="append", default=[], help="Confluence page URL (can repeat)")
    p.add_argument("--confluence-root", action="append", default=[], help="Root page URL to crawl descendants (can repeat)")
    p.add_argument("--max-pages", type=int, default=150)
    p.add_argument("--max-depth", type=int, default=3)

    # coveo
    p.add_argument("--coveo-org-id")
    p.add_argument("--coveo-platform-token")
    p.add_argument("--coveo-user-email")
    p.add_argument("--coveo-label", action="append", default=[], help="Coveo label to expand (can repeat)")

    # files
    p.add_argument("--file", action="append", default=[], help="Path to file (can repeat)")

    # report
    p.add_argument("--report", default="report.json", help="Where to write stats JSON")

    args = p.parse_args()

    run_pipeline(
        index_name=args.index,
        pinecone_secret_arn=args.pinecone_secret_arn,
        openai_secret_arn=args.openai_secret_arn,
        tenant=args.tenant,
        api_base=args.api_base,
        api_version=args.api_version,
        confluence_pages=args.confluence_page,
        confluence_roots=args.confluence_root,
        confluence_username=args.confluence_username,
        confluence_api_token=args.confluence_api_token,
        max_pages=args.max_pages,
        max_depth=args.max_depth,
        coveo_org_id=args.coveo_org_id,
        coveo_platform_token=args.coveo_platform_token,
        coveo_user_email=args.coveo_user_email,
        coveo_labels=args.coveo_label,
        files=args.file,
        report_path=args.report,
    )
