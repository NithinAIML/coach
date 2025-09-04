#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
pipeline.py  —  One-file data pipeline

What it does
------------
- Optionally pulls Confluence page URLs from Coveo tags + any explicit URLs you provide
- Fetches each Confluence page with LangChain ConfluenceLoader
- Optionally ingests local files (pdf/docx/txt/md/json/xml/csv/xlsx, etc.)
- Chunks text (RecursiveCharacterTextSplitter)
- Gets embeddings from Azure OpenAI (via Azure AD token using MSAL)
- Upserts vectors to Pinecone v3
- Writes a per-source + totals report (words/chunks/embeddings/index)

Run
---
python pipeline.py --config config.json --report report.json

Minimal config.json example (non-secret knobs only):
{
  "team_name": "Cloud Support",
  "index_name": "test-cloud-support",
  "embedding_deployment": "text-embedding-ada-002",      // your Azure *deployment name*
  "urls": [
    "https://company.atlassian.net/wiki/spaces/SPACE/pages/123456789/Page-Title"
  ],
  "files": [
    "docs/runbook.pdf",
    "docs/faq.docx",
    "docs/notes.txt"
  ],
  "coveo": { "tags": ["IAM", "Runbooks"] },               // optional; org/token/email come from coveo_config()
  "confluence": { "username": "user@company.com", "api_key": "your-confluence-token" },
  "limits": {
    "max_confluence_pages": 200,
    "max_chars_per_source": 200000
  }
}

Notes
-----
- All secrets are declared *below* in pinecone_config(), openai_api_config(), coveo_config().
- No environment variables and no AWS Secrets Manager in this version.
"""

import os
import re
import io
import sys
import json
import time
import math
import argparse
import hashlib
import warnings
from typing import List, Dict, Any, Tuple, Optional

# ---- Azure AD (MSAL) for Azure OpenAI ----
from msal import ConfidentialClientApplication

# ---- OpenAI (Azure) ----
import openai

# ---- Pinecone v3 ----
from pinecone import Pinecone, ServerlessSpec

# ---- Confluence ----
from langchain_community.document_loaders import ConfluenceLoader

# ---- HTTP/Coveo ----
import requests
from requests.auth import HTTPBasicAuth

# ---- Chunking ----
from langchain.text_splitter import RecursiveCharacterTextSplitter

# ---- Optional file parsers ----
try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

try:
    from pdfminer.high_level import extract_text as pdfminer_extract_text
except Exception:
    pdfminer_extract_text = None

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None

try:
    import docx  # python-docx
except Exception:
    docx = None

try:
    import pandas as pd
except Exception:
    pd = None

try:
    from lxml import etree as lxml_etree
except Exception:
    lxml_etree = None


# =========================================================
#                0) INLINE SECRETS / CONFIG
# =========================================================

def pinecone_config() -> Pinecone:
    """
    Put your Pinecone API key right here.
    """
    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    PINECONE_API_KEY = "YOUR_PINECONE_API_KEY"
    # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

    if not PINECONE_API_KEY or "YOUR_" in PINECONE_API_KEY:
        raise RuntimeError("Please set PINECONE_API_KEY in pinecone_config().")
    return Pinecone(api_key=PINECONE_API_KEY)


def openai_api_config() -> Tuple[str, str]:
    """
    Configure Azure OpenAI using an Azure AD token.
    Return (embedding_deployment_default, api_base) so caller can override if needed.
    """
    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    AZURE_TENANT = "YOUR_TENANT_ID_OR_NAME"
    AZURE_CLIENT_ID = "YOUR_AAD_APP_CLIENT_ID"
    AZURE_CLIENT_SECRET = "YOUR_AAD_APP_CLIENT_SECRET"
    AZURE_OPENAI_BASE = "https://<your-azure-openai>.openai.azure.com/"
    AZURE_OPENAI_API_VERSION = "2023-05-15"
    DEFAULT_EMBEDDING_DEPLOYMENT = "text-embedding-ada-002"  # your Azure *deployment* name
    # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

    if any("YOUR_" in s for s in [AZURE_TENANT, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET]) or "<your-azure-openai>" in AZURE_OPENAI_BASE:
        raise RuntimeError("Please fill Azure OpenAI credentials in openai_api_config().")

    # Acquire token
    scopes = ["https://cognitiveservices.azure.com/.default"]
    app = ConfidentialClientApplication(
        client_id=AZURE_CLIENT_ID,
        client_credential=AZURE_CLIENT_SECRET,
        authority=f"https://login.microsoftonline.com/{AZURE_TENANT}",
    )
    result = app.acquire_token_for_client(scopes=scopes)
    if "access_token" not in result:
        raise RuntimeError("Unable to obtain Azure AD token for OpenAI.")
    token = result["access_token"]

    # Configure OpenAI client
    openai.api_type = "azure_ad"
    openai.api_key = token
    openai.api_base = AZURE_OPENAI_BASE.rstrip("/") + "/"
    openai.api_version = AZURE_OPENAI_API_VERSION

    return DEFAULT_EMBEDDING_DEPLOYMENT, openai.api_base


def coveo_config() -> Dict[str, str]:
    """
    Coveo platform settings. Use these when config.json provides tags.
    """
    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    COVEO_ORG_ID = "YOUR_COVEO_ORG_ID"
    COVEO_PLATFORM_TOKEN = "YOUR_COVEO_PLATFORM_TOKEN"
    COVEO_USER_EMAIL = "user@company.com"
    # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    return {
        "organization_id": COVEO_ORG_ID,
        "auth_token": COVEO_PLATFORM_TOKEN,
        "user_email": COVEO_USER_EMAIL,
    }


# =========================================================
#                         Helpers
# =========================================================

def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def count_words(text: str) -> int:
    return len(re.findall(r"\w+", text or ""))


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def looks_like_url(s: str) -> bool:
    return isinstance(s, str) and (s.startswith("http://") or s.startswith("https://"))


# =========================================================
#                     Coveo + Confluence
# =========================================================

class CoveoSearch:
    def __init__(self, organization_id: str, auth_token: str):
        self.organization_id = organization_id
        self.auth_token = auth_token
        self.base_url = "https://platform.cloud.coveo.com/rest/search/v2"
        self.search_url = f"https://{organization_id}.org.coveo.com/rest/search/v2"

    def get_token(self, user_email: str) -> str:
        url = f"{self.base_url}/token"
        payload = {
            "organizationId": self.organization_id,
            "validFor": 180000,
            "userIds": [{"name": user_email, "provider": "Email Security Provider"}],
        }
        headers = {"authorization": f"Bearer {self.auth_token}", "content-type": "application/json"}
        r = requests.post(url, json=payload, headers=headers, timeout=30)
        r.raise_for_status()
        return r.json().get("token", "")

    def search_links(self, tag: str, token: str) -> List[str]:
        querystring = {"organizationId": self.organization_id}
        payload = {"q": f"@conflabels={tag}"}
        headers = {"authorization": f"Bearer {token}", "content-type": "application/json"}
        r = requests.post(self.search_url, json=payload, headers=headers, params=querystring, timeout=30)
        r.raise_for_status()
        return [res.get("clickUri") for res in (r.json().get("results") or []) if res.get("clickUri")]


def re_get_base_url(url: str) -> str:
    m = re.match(r"^(.*?)(?=/spaces)", url)
    if not m:
        raise ValueError("Base URL not matched for Confluence link")
    return m.group(1)


def re_get_page_id(url: str) -> str:
    m = re.search(r"pages/(\d+)", url)
    if not m:
        raise ValueError("No page ID found in Confluence URL")
    return m.group(1)


def fetch_confluence_pages(urls: List[str], username: str, api_key: str, max_pages: int = 200) -> Dict[str, str]:
    out: Dict[str, str] = {}
    count = 0
    for url in urls:
        if count >= max_pages:
            log("Reached max Confluence pages limit; stopping.")
            break
        try:
            page_id = str(re_get_page_id(url))
            base_url = re_get_base_url(url)
            loader = ConfluenceLoader(
                url=base_url,
                username=username,
                api_key=api_key,
                page_ids=[page_id],
                include_attachments=True,
            )
            docs = loader.load()
            if not docs:
                out[url] = ""
                continue
            text = (docs[0].metadata.get("title", "") or "") + ":\n" + (docs[0].page_content or "")
            text = re.sub(r"\n(?:\s*\n)+", "\n\n", text)
            out[url] = text
            count += 1
        except Exception as e:
            out[url] = f"Exception while fetching: {e}"
    return out


# =========================================================
#                    File extraction
# =========================================================

def read_pdf(path: str) -> str:
    if fitz is not None:
        try:
            parts = []
            with fitz.open(path) as doc:
                for page in doc:
                    parts.append(page.get_text("text") or "")
            txt = "\n".join(parts)
            if txt.strip():
                return txt
        except Exception:
            pass
    if pdfminer_extract_text is not None:
        try:
            txt = pdfminer_extract_text(path)
            if txt and txt.strip():
                return txt
        except Exception:
            pass
    if PdfReader is not None:
        try:
            reader = PdfReader(path)
            parts = []
            for p in getattr(reader, "pages", []):
                try:
                    parts.append(p.extract_text() or "")
                except Exception:
                    continue
            txt = "\n".join(parts)
            if txt.strip():
                return txt
        except Exception:
            pass
    return ""


def read_docx(path: str) -> str:
    if docx is None:
        return ""
    try:
        d = docx.Document(path)
        return "\n".join(par.text for par in d.paragraphs)
    except Exception:
        return ""


def read_textlike(path: str, encoding: Optional[str] = None) -> str:
    try:
        with open(path, "r", encoding=encoding or "utf-8", errors="ignore") as f:
            return f.read()
    except Exception:
        return ""


def read_json(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return json.dumps(data, indent=2, ensure_ascii=False)
    except Exception:
        return ""


def read_xml(path: str) -> str:
    if lxml_etree is None:
        return read_textlike(path)
    try:
        tree = lxml_etree.parse(path)
        return lxml_etree.tostring(tree, pretty_print=True, encoding="unicode")
    except Exception:
        return read_textlike(path)


def read_tabular(path: str) -> str:
    if pd is None:
        return ""
    try:
        if path.lower().endswith(".csv"):
            df = pd.read_csv(path)
        else:
            df = pd.read_excel(path)
        head = df.head(500)  # cap
        buf = io.StringIO()
        head.to_csv(buf, index=False)
        return buf.getvalue()
    except Exception:
        return ""


def read_any_file(path: str) -> str:
    p = path.lower()
    if p.endswith(".pdf"):
        return read_pdf(path)
    if p.endswith(".docx"):
        return read_docx(path)
    if p.endswith(".txt") or p.endswith(".md") or p.endswith(".log"):
        return read_textlike(path)
    if p.endswith(".json"):
        return read_json(path)
    if p.endswith(".xml"):
        return read_xml(path)
    if p.endswith(".csv") or p.endswith(".xlsx") or p.endswith(".xls"):
        return read_tabular(path)
    return read_textlike(path)


# =========================================================
#                Chunking & Embeddings & Pinecone
# =========================================================

def split_chunks(text: str, kind: str = "generic") -> List[str]:
    if kind in ("pdf", "doc", "docx"):
        chunk_size, overlap = 1200, 200
    elif kind in ("csv", "xlsx", "xls", "json", "xml"):
        chunk_size, overlap = 1500, 150
    else:
        chunk_size, overlap = 1000, 150
    splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=overlap)
    return splitter.split_text(text or "")


def embed_batch(texts: List[str], embedding_deployment: str) -> List[List[float]]:
    retries = 3
    for attempt in range(retries):
        try:
            resp = openai.Embedding.create(input=texts, engine=embedding_deployment)
            return [d["embedding"] for d in resp["data"]]
        except Exception as e:
            if attempt == retries - 1:
                log(f"ERROR embeddings: {e}")
                return []
            time.sleep(2 ** attempt)
    return []


def ensure_index(pc: Pinecone, index_name: str, dimension: int = 1536) -> None:
    try:
        names = [i.name for i in pc.list_indexes()]
    except Exception:
        names = []
    if index_name in names:
        return
    pc.create_index(
        name=index_name,
        dimension=dimension,
        metric="cosine",
        spec=ServerlessSpec(cloud="aws", region="us-east-1"),
        deletion_protection="disabled",
    )


def upsert_chunks_to_pinecone(
    pc: Pinecone,
    index_name: str,
    items: List[Tuple[str, str, Dict[str, Any]]],
    embedding_deployment: str,
    batch_size: int = 16,
) -> int:
    ensure_index(pc, index_name, dimension=1536)
    index = pc.Index(index_name)
    written = 0
    for i in range(0, len(items), batch_size):
        batch = items[i : i + batch_size]
        texts = [t for _, t, _ in batch]
        embs = embed_batch(texts, embedding_deployment)
        if not embs:
            continue
        vectors = [(id_, vec, md) for (id_, _, md), vec in zip(batch, embs)]
        index.upsert(vectors)
        written += len(vectors)
    return written


# =========================================================
#                       Pipeline
# =========================================================

def run_pipeline(config: Dict[str, Any]) -> Dict[str, Any]:
    # Secrets / clients from inline helpers
    pc = pinecone_config()
    default_embed_deploy, _ = openai_api_config()
    cov_defaults = coveo_config()

    team_name = config.get("team_name", "team")
    index_name = config.get("index_name") or f"test-{re.sub(r'[^a-z0-9-]+', '-', team_name.lower()).strip('-') or 'index'}"
    embedding_deployment = config.get("embedding_deployment", default_embed_deploy)

    limits = config.get("limits", {})
    max_pages = int(limits.get("max_confluence_pages", 200))
    max_chars_per_source = int(limits.get("max_chars_per_source", 200_000))

    # Confluence inputs
    conf_user = config.get("confluence", {}).get("username", "")
    conf_key = config.get("confluence", {}).get("api_key", "")

    # URLs from config + optionally from Coveo tags
    explicit_urls: List[str] = list(config.get("urls") or [])
    merged_urls: List[str] = []

    # Coveo by tags (org/token/email come from coveo_config above)
    tags = ((config.get("coveo") or {}).get("tags")) or []
    if tags:
        log("Querying Coveo tags…")
        cv = CoveoSearch(cov_defaults["organization_id"], cov_defaults["auth_token"])
        token = cv.get_token(cov_defaults["user_email"])
        for tag in tags:
            try:
                found = cv.search_links(tag, token)
                explicit_urls.extend(found)
            except Exception as e:
                log(f"Coveo tag '{tag}' search failed: {e}")

    seen = set()
    for u in explicit_urls:
        if u and u not in seen:
            merged_urls.append(u)
            seen.add(u)

    url_texts: Dict[str, str] = {}
    if merged_urls and conf_user and conf_key:
        log(f"Fetching Confluence pages: {len(merged_urls)}")
        url_texts = fetch_confluence_pages(merged_urls, conf_user, conf_key, max_pages=max_pages)

    # Files
    files: List[str] = config.get("files") or []
    file_texts: Dict[str, str] = {}
    for fp in files:
        if not os.path.exists(fp):
            log(f"File not found: {fp}")
            file_texts[fp] = ""
            continue
        text = read_any_file(fp)
        if text and len(text) > max_chars_per_source:
            log(f"Truncating very large source {fp} to {max_chars_per_source} chars")
            text = text[:max_chars_per_source]
        file_texts[fp] = text

    # Build items and report
    items: List[Tuple[str, str, Dict[str, Any]]] = []
    report_sources: Dict[str, Dict[str, Any]] = {}

    # URLs
    for u, text in url_texts.items():
        kind = "confluence"
        words = count_words(text)
        chunks = split_chunks(text, kind="doc")
        report_sources[u] = {"type": "url", "words": words, "chunks": len(chunks)}
        for i, ck in enumerate(chunks):
            cid = f"url:{sha1(u)}:{i}"
            md = {"source_url": u, "kind": kind, "chunk_index": i}
            items.append((cid, ck, md))

    # Files
    for path, text in file_texts.items():
        ext = os.path.splitext(path)[1].lower().lstrip(".")
        kind = ext or "file"
        words = count_words(text)
        chunks = split_chunks(text, kind=kind)
        report_sources[path] = {"type": "file", "words": words, "chunks": len(chunks)}
        for i, ck in enumerate(chunks):
            cid = f"file:{sha1(os.path.abspath(path))}:{i}"
            md = {"source_file": os.path.abspath(path), "kind": kind, "chunk_index": i}
            items.append((cid, ck, md))

    log(f"Total chunks to embed: {len(items)}")

    written = upsert_chunks_to_pinecone(
        pc=pc,
        index_name=index_name,
        items=items,
        embedding_deployment=embedding_deployment,
        batch_size=16,
    )
    log(f"Vectors written: {written}")

    totals = {
        "sources": len(report_sources),
        "chunks": sum(s["chunks"] for s in report_sources.values()),
        "words": sum(s["words"] for s in report_sources.values()),
        "embeddings_written": written,
        "index_name": index_name,
    }
    return {"sources": report_sources, "totals": totals}


# =========================================================
#                         CLI
# =========================================================

def main():
    parser = argparse.ArgumentParser(description="Coveo/Confluence + Files -> Chunks -> Azure OpenAI embeddings -> Pinecone v3")
    parser.add_argument("--config", required=True, help="Path to config.json")
    parser.add_argument("--report", required=True, help="Path to write report.json")
    args = parser.parse_args()

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    out = run_pipeline(cfg)

    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    log(f"Done. Report written to {args.report}")


if __name__ == "__main__":
    main()
