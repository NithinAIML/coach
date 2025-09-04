#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
pipeline.py — single-file pipeline with runtime secrets from AWS Secrets Manager

What this does
--------------
1) Loads secrets at runtime from AWS Secrets Manager:
   - Pinecone: expects JSON with key "apiKey"
   - Azure OpenAI AAD app: expects JSON with fields like
       {
         "AzureServicePrincipalId": "...",     # client_id
         "Password": "...",                    # client_secret
         "TenantId": "xxxxxxxx-xxxx-...",      # or "TenantName": "contoso.onmicrosoft.com"
         "Endpoint": "https://<resource>.openai.azure.com",
         "ApiVersion": "2024-02-01",
         "EmbeddingDeployment": "text-embedding-3-small",
         "EmbeddingDimension": 1536
       }
   (Strips stray quotes if secrets are double-quoted inside the SecretString.)

2) Builds embeddings via Azure OpenAI (AAD token with MSAL), chunked text.
3) Ingests:
   - Confluence root (space) or single page via ConfluenceLoader
   - Optional Coveo discovery by labels (adds discovered Confluence pages)
   - Files: pdf (PyMuPDF→PyPDF2→pdfminer), docx, doc (textract optional), txt/md, html, json, csv, xlsx
4) Adaptive chunking (RecursiveCharacterTextSplitter), per-source tuning
5) Stores embeddings to Pinecone index (default: test-{team_name})
6) Prints a report: per-source words & chunk counts + totals (and discovered URLs if any)

Usage
-----
python pipeline.py config.json

Example config.json:
{
  "team_name": "IAM",
  "index_name": "test-iam",
  "confluence": {
    "username": "user@company.com",
    "api_token": "CONFLUENCE_API_TOKEN",
    "sources": [
      {"type":"root","url":"https://your.atlassian.net/wiki/spaces/IAM/overview"},
      {"type":"page","url":"https://your.atlassian.net/wiki/spaces/IAM/pages/123456789/Page"}
    ],
    "include_attachments": true,
    "max_space_pages": 300
  },
  "coveo": {
    "enabled": false,
    "organization_id": "your-org-id",
    "auth_token": "COVEO_PLATFORM_TOKEN",
    "user_email": "user@company.com",
    "tags": ["kb-label"]
  },
  "files": ["./docs/a.pdf","./docs/b.docx","./docs/sheet.xlsx"]
}

Install (typical)
-----------------
pip install msal openai pinecone-client langchain-community pandas beautifulsoup4 PyMuPDF pypdf2 pdfminer.six python-docx
# optional: textract (for .doc) if system deps are present

"""

import os
import re
import io
import sys
import json
import time
import traceback
from typing import Any, Dict, List, Optional, Tuple

# ======================== Inline config (edit ARNs) ========================
CONFIG: Dict[str, Any] = {
    "aws_region": "us-east-1",
    "secrets": {
        # <<< REPLACE THESE WITH YOUR SECRET ARNs >>>
        "pinecone_secret_arn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:pinecone/apiKey-XXXXX",
        "azure_openai_secret_arn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:azure-openai/app-XXXXX",
    },
    # Optional defaults if your secret doesn’t include these (will be overridden by secret values if present):
    "azure_defaults": {
        "api_version": "2024-02-01",
        "embedding_deployment": "text-embedding-3-small",
        "embedding_dimension": 1536,
    },
    # Pinecone serverless region (matches your project)
    "pinecone_region": "us-east-1",
}
# ==========================================================================


# ========================== Secrets via AWS SM =============================
def _strip_quotes(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    return s.replace('"', '').replace("'", '').strip()


def _load_secret_json(arn: str, region: str) -> Dict[str, Any]:
    import boto3
    sm = boto3.client(service_name="secretsmanager", region_name=region)
    resp = sm.get_secret_value(SecretId=arn)
    raw = resp.get("SecretString") or ""
    try:
        data = json.loads(raw)
    except Exception:
        # sometimes the secret is stored as a quoted string of JSON inside JSON
        raw2 = _strip_quotes(raw) or ""
        try:
            data = json.loads(raw2)
        except Exception:
            data = {}
    return data


def load_runtime_secrets() -> Dict[str, Any]:
    """
    Returns a dict:
    {
      "pinecone_api_key": "...",
      "azure": {
        "tenant": "...", "client_id": "...", "client_secret": "...",
        "endpoint": "https://...azure.com", "api_version": "2024-02-01",
        "embedding_deployment": "text-embedding-3-small",
        "embedding_dimension": 1536
      }
    }
    """
    region = CONFIG.get("aws_region", "us-east-1")
    out = {"pinecone_api_key": None, "azure": {}}

    # Pinecone secret
    pc_arn = CONFIG["secrets"].get("pinecone_secret_arn")
    if pc_arn:
        pc_json = _load_secret_json(pc_arn, region)
        api_key = pc_json.get("apiKey")
        if api_key is None:
            # also try "apikey"
            api_key = pc_json.get("apikey")
        out["pinecone_api_key"] = _strip_quotes(api_key)
    else:
        out["pinecone_api_key"] = None

    # Azure OpenAI / AAD app secret
    az_arn = CONFIG["secrets"].get("azure_openai_secret_arn")
    az: Dict[str, Any] = {}
    if az_arn:
        az_json = _load_secret_json(az_arn, region)

        # Client creds
        client_id = az_json.get("AzureServicePrincipalId") or az_json.get("ClientId")
        client_secret = az_json.get("Password") or az_json.get("ClientSecret")
        tenant = az_json.get("TenantId") or az_json.get("TenantName")  # allow either

        # Service config
        endpoint = az_json.get("Endpoint") or az_json.get("endpoint")  # https://<resource>.openai.azure.com
        api_version = az_json.get("ApiVersion") or CONFIG["azure_defaults"]["api_version"]
        emb_dep = az_json.get("EmbeddingDeployment") or CONFIG["azure_defaults"]["embedding_deployment"]
        emb_dim = az_json.get("EmbeddingDimension") or CONFIG["azure_defaults"]["embedding_dimension"]

        az = {
            "tenant": _strip_quotes(tenant),
            "client_id": _strip_quotes(client_id),
            "client_secret": _strip_quotes(client_secret),
            "endpoint": _strip_quotes(endpoint),
            "api_version": _strip_quotes(str(api_version)),
            "embedding_deployment": _strip_quotes(emb_dep),
            "embedding_dimension": int(emb_dim) if str(emb_dim).isdigit() else CONFIG["azure_defaults"]["embedding_dimension"],
        }
    out["azure"] = az
    return out
# ==========================================================================


# ============================= Embeddings ================================
def _get_az_aad_token(tenant: str, client_id: str, client_secret: str) -> str:
    """Acquire AAD token (client credentials) for Azure Cognitive Services."""
    from msal import ConfidentialClientApplication
    authority = f"https://login.microsoftonline.com/{tenant}"
    app = ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority,
    )
    scopes = ["https://cognitiveservices.azure.com/.default"]
    res = app.acquire_token_for_client(scopes=scopes)
    if "access_token" not in res:
        raise RuntimeError(f"MSAL token fetch failed: {res}")
    return res["access_token"]


def _embedding_client_from_secrets(az: Dict[str, Any]):
    """
    Returns a tuple (flavor, client, model), supporting both new and legacy OpenAI SDKs.
    Uses AAD token as API key for AzureOpenAI (new SDK), or sets legacy globals if needed.
    """
    endpoint = az["endpoint"]
    api_version = az["api_version"]
    model = az["embedding_deployment"]

    # Acquire token
    token = _get_az_aad_token(az["tenant"], az["client_id"], az["client_secret"])

    # Prefer new SDK if available
    try:
        from openai import AzureOpenAI
        client = AzureOpenAI(
            azure_endpoint=endpoint,
            api_version=api_version,
            api_key=token,  # bearer in new SDK
        )
        return ("new-azure", client, model)
    except Exception:
        pass

    # Fallback: legacy openai
    import openai as _openai
    _openai.api_type = "azure_ad"
    _openai.api_base = endpoint
    _openai.api_version = api_version
    _openai.api_key = token
    return ("legacy-azure", _openai, model)


def embed_texts(texts: List[str], az_cfg: Dict[str, Any]) -> List[List[float]]:
    if not texts:
        return []
    flavor, client, model = _embedding_client_from_secrets(az_cfg)
    retries, backoff = 4, 2.0
    for attempt in range(1, retries + 1):
        try:
            if flavor == "new-azure":
                resp = client.embeddings.create(model=model, input=texts)
                return [d.embedding for d in resp.data]
            else:
                resp = client.Embedding.create(engine=model, input=texts)
                return [d["embedding"] for d in resp["data"]]
        except Exception as e:
            if attempt == retries:
                raise
            time.sleep(backoff ** attempt)
    return []
# ==========================================================================


# ============================== Pinecone ================================
from pinecone import Pinecone, ServerlessSpec
try:
    from pinecone.exceptions import PineconeApiException
except Exception:
    try:
        from pinecone.exceptions.exceptions import PineconeApiException
    except Exception:
        class PineconeApiException(Exception):  # type: ignore
            pass


def pc_client(pinecone_api_key: str) -> Pinecone:
    return Pinecone(api_key=pinecone_api_key)


def ensure_index(pc: Pinecone, index_name: str, dimension: int, region: str) -> None:
    try:
        pc.describe_index(index_name)
        return
    except PineconeApiException as e:
        if "NOT_FOUND" not in str(e) and "404" not in str(e):
            pass
    except Exception:
        pass
    try:
        pc.create_index(
            name=index_name,
            dimension=dimension,
            metric="cosine",
            spec=ServerlessSpec(cloud="aws", region=region),
            deletion_protection="disabled",
        )
    except PineconeApiException as e:
        if "ALREADY" not in str(e) and "409" not in str(e):
            raise
    pc.describe_index(index_name)


def private_host(pc: Pinecone, index_name: str) -> str:
    desc = pc.describe_index(index_name)
    host = desc["host"]
    parts = host.split(".")
    return f"{'.'.join(parts[:2])}.private.{'.'.join(parts[2:])}"


def upsert_to_pinecone(pinecone_api_key: str,
                       region: str,
                       index_name: str,
                       dimension: int,
                       vectors: List[Tuple[str, List[float], Dict[str, Any]]]) -> None:
    pc = pc_client(pinecone_api_key)
    ensure_index(pc, index_name, dimension, region)
    host = private_host(pc, index_name)
    index = pc.Index(host=host)
    B = 100
    for i in range(0, len(vectors), B):
        index.upsert(vectors[i:i + B])
# ==========================================================================


# =========================== Confluence / Coveo ==========================
def get_base_url(url: str) -> str:
    m = re.match(r"^(.*?)(?=\/spaces|\/wiki)", url)
    if m:
        return m.group(1)
    raise ValueError("Base URL not matched")


def get_page_id(url: str) -> str:
    m = re.search(r"pages\/(\d+)", url)
    if m:
        return m.group(1)
    raise ValueError("No page ID found in URL")


def get_space_key(url: str) -> Optional[str]:
    m = re.search(r"/spaces/([A-Z0-9]+)/", url, flags=re.I)
    return m.group(1) if m else None


def remove_repeated_newlines(s: str) -> str:
    return re.sub(r"\n(?:[\t ]*\n)+", "\n\n", s)


# Coveo discovery
import requests
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
            "validFor": 180000,  # ms
            "userIds": [{"name": user_email, "provider": "Email Security Provider"}],
        }
        headers = {"authorization": f"Bearer {self.auth_token}", "content-type": "application/json"}
        r = requests.post(url, json=payload, headers=headers, timeout=30)
        r.raise_for_status()
        return r.json().get("token", "")

    def search_links(self, label: str, user_token: str) -> List[str]:
        params = {"organizationId": self.organization_id}
        payload = {"q": f"@conflabels={label}"}
        headers = {"authorization": f"Bearer {user_token}", "content-type": "application/json"}
        r = requests.post(self.search_url, json=payload, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json() or {}
        return [it.get("clickUri") for it in data.get("results", []) if it.get("clickUri")]
# ==========================================================================


# ================================ Ingest ================================
def ingest_confluence(sources: List[Dict[str, Any]],
                      username: str,
                      api_token: str,
                      include_attachments: bool = True,
                      max_space_pages: int = 1000) -> List[Dict[str, Any]]:
    """
    sources: items like:
      {"type":"root","url":"https://<site>/spaces/SPACE/overview"} -> whole space (limit)
      {"type":"page","url":"https://<site>/spaces/SPACE/pages/<id>/..."} -> that page
    """
    from langchain_community.document_loaders import ConfluenceLoader
    docs: List[Dict[str, Any]] = []
    for src in sources:
        url = src.get("url") or ""
        if not url:
            continue
        base = get_base_url(url)
        space = get_space_key(url)
        try:
            if src.get("type") == "root" and space:
                loader = ConfluenceLoader(
                    url=base,
                    username=username,
                    api_key=api_token,
                    space_key=space,
                    include_attachments=include_attachments,
                    limit=max_space_pages
                )
                items = loader.load()
                for d in items:
                    text = remove_repeated_newlines(d.page_content or "")
                    meta = dict(d.metadata or {})
                    pid = meta.get("id") or meta.get("page_id") or f"{space}:{hash(text)}"
                    docs.append({
                        "id": f"url:{base}/spaces/{space}/{pid}",
                        "text": text,
                        "meta": {"source_type": "confluence", "space": space, **meta}
                    })
            else:
                page_id = get_page_id(url)
                loader = ConfluenceLoader(
                    url=base,
                    username=username,
                    api_key=api_token,
                    page_ids=[page_id],
                    include_attachments=include_attachments
                )
                items = loader.load()
                for d in items:
                    text = remove_repeated_newlines(d.page_content or "")
                    meta = dict(d.metadata or {})
                    docs.append({
                        "id": f"url:{url}",
                        "text": text,
                        "meta": {"source_type": "confluence", **meta}
                    })
        except Exception as e:
            print(f"[WARN] Confluence ingest failed for {url}: {e}")
    return docs


def discover_with_coveo(conf: Dict[str, Any]) -> List[str]:
    """conf: {organization_id, auth_token, user_email, tags:[...]} -> urls"""
    try:
        org = conf["organization_id"]; tok = conf["auth_token"]; user = conf["user_email"]
        tags = conf.get("tags") or []
        cv = CoveoSearch(org, tok)
        user_token = cv.get_token(user)
        urls: List[str] = []
        for t in tags:
            urls.extend(cv.search_links(t, user_token))
        # de-dupe
        seen, out = set(), []
        for u in urls:
            if u and u not in seen:
                seen.add(u); out.append(u)
        return out
    except Exception as e:
        print(f"[WARN] Coveo discovery failed: {e}")
        return []


# ----- File readers -----
def read_pdf(path: str) -> List[Tuple[str, str, Dict[str, Any]]]:
    pages: List[Tuple[str, str, Dict[str, Any]]] = []
    # PyMuPDF first
    try:
        import fitz  # PyMuPDF
        with fitz.open(path) as doc:
            for i, page in enumerate(doc):
                text = page.get_text("text") or page.get_text()
                pages.append((f"file:{path}#p{i+1}", text or "", {"source_type": "file", "filename": os.path.basename(path), "page": i+1}))
        return pages
    except Exception:
        pass
    # PyPDF2 next
    try:
        import PyPDF2
        with open(path, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            for i, pg in enumerate(reader.pages):
                text = pg.extract_text() or ""
                pages.append((f"file:{path}#p{i+1}", text, {"source_type": "file", "filename": os.path.basename(path), "page": i+1}))
        return pages
    except Exception:
        pass
    # pdfminer fallback
    try:
        from pdfminer.high_level import extract_text
        text = extract_text(path)
        pages.append((f"file:{path}", text or "", {"source_type": "file", "filename": os.path.basename(path)}))
        return pages
    except Exception as e:
        print(f"[WARN] PDF read failed for {path}: {e}")
        return []


def read_docx(path: str) -> str:
    try:
        import docx
        d = docx.Document(path)
        return "\n".join(p.text for p in d.paragraphs if p.text)
    except Exception as e:
        print(f"[WARN] DOCX read failed for {path}: {e}")
        return ""


def read_doc(path: str) -> str:
    try:
        import textract  # optional
        content = textract.process(path)
        return content.decode("utf-8", errors="ignore")
    except Exception as e:
        print(f"[WARN] .doc read failed for {path}: {e}")
        return ""


def read_text(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    except Exception as e:
        print(f"[WARN] text read failed for {path}: {e}")
        return ""


def read_html(path: str) -> str:
    try:
        from bs4 import BeautifulSoup
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            html = f.read()
        soup = BeautifulSoup(html, "html.parser")
        return soup.get_text(separator="\n")
    except Exception as e:
        print(f"[WARN] html read failed for {path}: {e}")
        return ""


def read_json(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            data = json.load(f)
        def _flatten(x):
            if isinstance(x, dict):
                return " ".join(_flatten(v) for v in x.values())
            if isinstance(x, list):
                return " ".join(_flatten(v) for v in x)
            return str(x)
        return _flatten(data)
    except Exception as e:
        print(f"[WARN] json read failed for {path}: {e}")
        return ""


def read_csv(path: str) -> str:
    try:
        import pandas as pd  # type: ignore
        df = pd.read_csv(path)
        return df.to_csv(index=False)
    except Exception as e:
        print(f"[WARN] csv read failed for {path}: {e}")
        return ""


def read_xlsx(path: str) -> str:
    try:
        import pandas as pd  # type: ignore
        xl = pd.ExcelFile(path)
        chunks = []
        for name in xl.sheet_names:
            df = xl.parse(name)
            buf = io.StringIO()
            buf.write(f"# Sheet: {name}\n")
            buf.write(df.to_csv(index=False))
            chunks.append(buf.getvalue())
        return "\n\n".join(chunks)
    except Exception as e:
        print(f"[WARN] xlsx read failed for {path}: {e}")
        return ""


def ingest_files(paths: List[str]) -> List[Dict[str, Any]]:
    docs: List[Dict[str, Any]] = []
    for p in paths:
        ext = os.path.splitext(p)[1].lower()
        if ext == ".pdf":
            pages = read_pdf(p)
            for pid, text, meta in pages:
                docs.append({"id": pid, "text": text or "", "meta": meta})
        elif ext == ".docx":
            text = read_docx(p)
            docs.append({"id": f"file:{p}", "text": text, "meta": {"source_type": "file", "filename": os.path.basename(p)}})
        elif ext == ".doc":
            text = read_doc(p)
            docs.append({"id": f"file:{p}", "text": text, "meta": {"source_type": "file", "filename": os.path.basename(p)}})
        elif ext in (".txt", ".md"):
            text = read_text(p)
            docs.append({"id": f"file:{p}", "text": text, "meta": {"source_type": "file", "filename": os.path.basename(p)}})
        elif ext in (".html", ".htm"):
            text = read_html(p)
            docs.append({"id": f"file:{p}", "text": text, "meta": {"source_type": "file", "filename": os.path.basename(p)}})
        elif ext == ".json":
            text = read_json(p)
            docs.append({"id": f"file:{p}", "text": text, "meta": {"source_type": "file", "filename": os.path.basename(p)}})
        elif ext == ".csv":
            text = read_csv(p)
            docs.append({"id": f"file:{p}", "text": text, "meta": {"source_type": "file", "filename": os.path.basename(p)}})
        elif ext in (".xlsx", ".xlsm", ".xls"):
            text = read_xlsx(p)
            docs.append({"id": f"file:{p}", "text": text, "meta": {"source_type": "file", "filename": os.path.basename(p)}})
        else:
            print(f"[WARN] unsupported file type: {p}")
    return docs
# ==========================================================================


# =========================== Chunking / utils ============================
from langchain.text_splitter import RecursiveCharacterTextSplitter

def _adaptive_splitter(meta: Dict[str, Any]) -> RecursiveCharacterTextSplitter:
    st = meta.get("source_type")
    if st == "confluence":
        return RecursiveCharacterTextSplitter(chunk_size=1200, chunk_overlap=200)
    if st == "file":
        fname = (meta.get("filename") or "").lower()
        if fname.endswith(".pdf"):
            return RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=180)
        if fname.endswith((".csv", ".xlsx", ".xls")):
            return RecursiveCharacterTextSplitter(chunk_size=1800, chunk_overlap=200)
        if fname.endswith((".json",)):
            return RecursiveCharacterTextSplitter(chunk_size=900, chunk_overlap=150)
        if fname.endswith((".md", ".txt")):
            return RecursiveCharacterTextSplitter(chunk_size=1200, chunk_overlap=200)
        return RecursiveCharacterTextSplitter(chunk_size=1100, chunk_overlap=180)
    return RecursiveCharacterTextSplitter(chunk_size=1100, chunk_overlap=180)


def chunk_documents(documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    chunks: List[Dict[str, Any]] = []
    for doc in documents:
        text = (doc.get("text") or "").strip()
        if not text:
            continue
        splitter = _adaptive_splitter(doc.get("meta") or {})
        parts = splitter.split_text(text)
        for i, part in enumerate(parts):
            cid = f"{doc.get('id','doc')}::ch{i}"
            meta = dict(doc.get("meta") or {})
            meta["chunk_index"] = i
            meta["source_id"] = doc.get("id")
            meta["char_len"] = len(part)
            chunks.append({"id": cid, "text": part, "meta": meta})
    return chunks
# ==========================================================================


# =============================== Runner ==================================
def build_report(docs: List[Dict[str, Any]], chunks: List[Dict[str, Any]]) -> Dict[str, Any]:
    per_source: Dict[str, Dict[str, Any]] = {}
    for d in docs:
        sid = d["id"]
        words = len((d.get("text") or "").split())
        meta = d.get("meta") or {}
        per_source[sid] = {
            "source_type": meta.get("source_type"),
            "space": meta.get("space"),
            "filename": meta.get("filename"),
            "page": meta.get("page"),
            "words": words,
            "chunks": 0,
        }
    for c in chunks:
        sid = c["meta"].get("source_id")
        if sid in per_source:
            per_source[sid]["chunks"] += 1
    totals = {
        "sources": len(docs),
        "total_words": sum(v["words"] for v in per_source.values()),
        "total_chunks": sum(v["chunks"] for v in per_source.values()),
    }
    return {"per_source": per_source, "totals": totals}


def store_text_corpus(index_name: str,
                      corpus: List[Dict[str, Any]],
                      az_cfg: Dict[str, Any],
                      pinecone_api_key: str,
                      pinecone_region: str) -> Dict[str, Any]:
    # 1) chunk
    chunks = chunk_documents(corpus)

    # 2) embed + collect vectors
    vectors: List[Tuple[str, List[float], Dict[str, Any]]] = []
    B = 64
    for i in range(0, len(chunks), B):
        batch = chunks[i:i + B]
        texts = [c["text"] for c in batch]
        embs = embed_texts(texts, az_cfg)
        for c, e in zip(batch, embs):
            vectors.append((c["id"], e, c["meta"]))

    # 3) upsert
    dim = az_cfg.get("embedding_dimension", 1536)
    upsert_to_pinecone(pinecone_api_key, pinecone_region, index_name, dim, vectors)

    report = build_report(corpus, chunks)
    report.update({"index": index_name, "upserted": len(vectors)})
    return report


def run_pipeline(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    cfg example in module docstring.
    """
    # 0) Load secrets at runtime
    secrets = load_runtime_secrets()
    pinecone_api_key = secrets.get("pinecone_api_key")
    az_cfg = secrets.get("azure", {})

    if not pinecone_api_key:
        raise RuntimeError("Pinecone API key not found in Secrets Manager (apiKey).")
    for k in ("tenant", "client_id", "client_secret", "endpoint", "api_version", "embedding_deployment"):
        if not az_cfg.get(k):
            raise RuntimeError(f"Azure OpenAI secret missing required field: {k}")

    # 1) Prep index name
    team = (cfg.get("team_name") or "team").strip().lower().replace(" ", "-")
    index_name = (cfg.get("index_name") or f"test-{team}").lower()

    all_docs: List[Dict[str, Any]] = []
    discovered_urls: List[str] = []

    # 2) Optional Coveo discovery
    coveo_conf = cfg.get("coveo") or {}
    if coveo_conf.get("enabled"):
        discovered_urls = discover_with_coveo(coveo_conf)
        if discovered_urls:
            conf = cfg.get("confluence") or {}
            srcs = conf.get("sources") or []
            for u in discovered_urls:
                srcs.append({"type": "page", "url": u})
            conf["sources"] = srcs
            cfg["confluence"] = conf

    # 3) Confluence ingestion
    conf = cfg.get("confluence") or {}
    if conf.get("sources") and conf.get("username") and conf.get("api_token"):
        docs_c = ingest_confluence(
            sources=conf["sources"],
            username=conf["username"],
            api_token=conf["api_token"],
            include_attachments=bool(conf.get("include_attachments", True)),
            max_space_pages=int(conf.get("max_space_pages", 1000)),
        )
        all_docs.extend(docs_c)

    # 4) Files ingestion
    files = cfg.get("files") or []
    if files:
        docs_f = ingest_files(files)
        all_docs.extend(docs_f)

    # 5) Store in Pinecone
    report = store_text_corpus(
        index_name=index_name,
        corpus=all_docs,
        az_cfg=az_cfg,
        pinecone_api_key=pinecone_api_key,
        pinecone_region=CONFIG.get("pinecone_region", "us-east-1"),
    )
    report["discovered_urls"] = discovered_urls
    return report
# ==========================================================================


# ================================= CLI ===================================
def _read_json_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    if len(sys.argv) < 2:
        print("Usage: python pipeline.py <config.json>\n")
        print("See module docstring for a full example config.")
        sys.exit(1)

    cfg = _read_json_file(sys.argv[1])
    try:
        out = run_pipeline(cfg)
        print(json.dumps(out, indent=2))
    except Exception:
        print("[ERROR] Pipeline failed:")
        traceback.print_exc()
        sys.exit(2)


if __name__ == "__main__":
    main()
