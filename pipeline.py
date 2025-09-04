#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
pipeline.py — One-file pipeline
- Inline secrets/config (Azure AD + Azure OpenAI OR plain OpenAI) + Pinecone
- Optional Coveo discovery; Confluence fetching with ConfluenceLoader
- File ingestion (pdf/docx/doc/txt/md/html/json/csv/xlsx) with fallbacks
- Adaptive chunking, embeddings, Pinecone upsert
- Detailed per-source + total stats
- CLI: python pipeline.py config.json
"""

import os
import re
import io
import sys
import json
import time
import math
import hashlib
import traceback
from typing import Any, Dict, List, Optional, Tuple

# ===================== Inline secrets (edit these) =====================
CONFIG = {
    # Embeddings provider:
    #   True  -> Azure OpenAI with AAD via MSAL (tenant/client/secret)
    #   False -> Plain OpenAI API key
    "use_azure_openai": True,

    # Azure AD + Azure OpenAI settings
    "azure_openai": {
        "tenant_id":     "YOUR_TENANT_ID_OR_NAME",      # GUID or "contoso.onmicrosoft.com"
        "client_id":     "YOUR_APP_REG_CLIENT_ID",
        "client_secret": "YOUR_APP_REG_CLIENT_SECRET",

        # Azure OpenAI resource
        "endpoint":      "https://YOUR-RESOURCE.openai.azure.com",
        "api_version":   "2024-02-01",

        # Embedding deployment name (1536-dim recommended)
        "embedding_deployment": "text-embedding-3-small",
        "embedding_dimension": 1536,

        # MSAL scope for Azure Cognitive Services
        "scopes": ["https://cognitiveservices.azure.com/.default"],
    },

    # Plain OpenAI settings (if not using Azure)
    "openai": {
        "api_key": "",  # "sk-..."
        "embedding_model": "text-embedding-3-small",
        "embedding_dimension": 1536,
    },

    # Pinecone
    "pinecone": {
        "api_key": "pc-XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
        "serverless_region": "us-east-1",
    },
}
# ======================================================================


# =============== Bootstrap providers from CONFIG/env ==================
def _apply_inline_config_to_env() -> None:
    use_az = CONFIG.get("use_azure_openai", False)

    # Pinecone
    if not os.getenv("PINECONE_API_KEY") and CONFIG["pinecone"].get("api_key"):
        os.environ["PINECONE_API_KEY"] = CONFIG["pinecone"]["api_key"]
    if not os.getenv("PINECONE_SERVERLESS_REGION") and CONFIG["pinecone"].get("serverless_region"):
        os.environ["PINECONE_SERVERLESS_REGION"] = CONFIG["pinecone"]["serverless_region"]

    if use_az:
        az = CONFIG["azure_openai"]
        if not os.getenv("AZURE_OPENAI_ENDPOINT") and az.get("endpoint"):
            os.environ["AZURE_OPENAI_ENDPOINT"] = az["endpoint"]
        if not os.getenv("AZURE_OPENAI_API_VERSION") and az.get("api_version"):
            os.environ["AZURE_OPENAI_API_VERSION"] = az["api_version"]
        if not os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT") and az.get("embedding_deployment"):
            os.environ["AZURE_OPENAI_EMBEDDING_DEPLOYMENT"] = az["embedding_deployment"]
        if not os.getenv("AZURE_OPENAI_EMBEDDING_DIMENSION") and az.get("embedding_dimension"):
            os.environ["AZURE_OPENAI_EMBEDDING_DIMENSION"] = str(az["embedding_dimension"])
    else:
        oa = CONFIG["openai"]
        if not os.getenv("OPENAI_API_KEY") and oa.get("api_key"):
            os.environ["OPENAI_API_KEY"] = oa["api_key"]
        if not os.getenv("OPENAI_EMBEDDING_MODEL") and oa.get("embedding_model"):
            os.environ["OPENAI_EMBEDDING_MODEL"] = oa["embedding_model"]
        if not os.getenv("OPENAI_EMBEDDING_DIMENSION") and oa.get("embedding_dimension"):
            os.environ["OPENAI_EMBEDDING_DIMENSION"] = str(oa["embedding_dimension"])


_apply_inline_config_to_env()
# ======================================================================


# ============================ Embeddings ===============================
def _get_azure_aad_token() -> str:
    """Acquire an AAD token for Azure Cognitive Services using MSAL confidential client."""
    from msal import ConfidentialClientApplication
    az = CONFIG["azure_openai"]
    tenant = az["tenant_id"]
    authority = f"https://login.microsoftonline.com/{tenant}"
    app = ConfidentialClientApplication(
        client_id=az["client_id"],
        client_credential=az["client_secret"],
        authority=authority,
    )
    scopes = az.get("scopes") or ["https://cognitiveservices.azure.com/.default"]
    result = app.acquire_token_for_client(scopes=scopes)
    if "access_token" not in result:
        raise RuntimeError(f"MSAL acquire_token_for_client failed: {result}")
    return result["access_token"]


def _get_embedding_client():
    """Return (flavor, client) handling both new and legacy OpenAI SDKs."""
    use_az = CONFIG.get("use_azure_openai", False)
    try:
        if use_az:
            from openai import AzureOpenAI
            client = AzureOpenAI(
                azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
                api_version=os.environ["AZURE_OPENAI_API_VERSION"],
                api_key=_get_azure_aad_token(),  # Bearer token via api_key in new SDK
            )
            return ("new-azure", client)
        else:
            from openai import OpenAI
            client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
            return ("new-openai", client)
    except Exception:
        import openai as _openai
        if use_az:
            _openai.api_type = "azure_ad"
            _openai.api_base = os.environ["AZURE_OPENAI_ENDPOINT"]
            _openai.api_version = os.environ["AZURE_OPENAI_API_VERSION"]
            _openai.api_key = _get_azure_aad_token()
            return ("legacy-azure", _openai)
    # legacy openai
    import openai as _openai
    _openai.api_key = os.environ["OPENAI_API_KEY"]
    return ("legacy-openai", _openai)


def embed_texts(texts: List[str]) -> List[List[float]]:
    """Create embeddings with retries."""
    if not texts:
        return []
    flavor, client = _get_embedding_client()
    model = os.environ.get("AZURE_OPENAI_EMBEDDING_DEPLOYMENT") if CONFIG.get("use_azure_openai", False) \
        else os.environ.get("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
    retries, backoff = 4, 2.0
    for attempt in range(1, retries + 1):
        try:
            if flavor in ("new-azure", "new-openai"):
                resp = client.embeddings.create(model=model, input=texts)
                return [d.embedding for d in resp.data]
            elif flavor == "legacy-azure":
                resp = client.Embedding.create(engine=model, input=texts)
                return [d["embedding"] for d in resp["data"]]
            else:  # legacy-openai
                resp = client.Embedding.create(model=model, input=texts)
                return [d["embedding"] for d in resp["data"]]
        except Exception as e:
            if attempt == retries:
                raise
            time.sleep(backoff ** attempt)
    return []
# ======================================================================


# ============================ Pinecone ================================
from pinecone import Pinecone, ServerlessSpec
try:
    from pinecone.exceptions import PineconeApiException
except Exception:
    try:
        from pinecone.exceptions.exceptions import PineconeApiException
    except Exception:
        class PineconeApiException(Exception):
            pass


def _pc_client() -> Pinecone:
    return Pinecone(api_key=os.environ["PINECONE_API_KEY"])


def ensure_index(index_name: str, dimension: int = 1536) -> None:
    pc = _pc_client()
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
            spec=ServerlessSpec(cloud="aws", region=os.environ.get("PINECONE_SERVERLESS_REGION", "us-east-1")),
            deletion_protection="disabled",
        )
    except PineconeApiException as e:
        if "ALREADY" not in str(e) and "409" not in str(e):
            raise
    pc.describe_index(index_name)


def _private_host(pc: Pinecone, index_name: str) -> str:
    desc = pc.describe_index(index_name)
    host = desc["host"]
    parts = host.split(".")
    return f"{'.'.join(parts[:2])}.private.{'.'.join(parts[2:])}"


def upsert_embeddings(index_name: str, vectors: List[Tuple[str, List[float], Dict[str, Any]]]) -> None:
    pc = _pc_client()
    dim = int(os.environ.get("AZURE_OPENAI_EMBEDDING_DIMENSION") or os.environ.get("OPENAI_EMBEDDING_DIMENSION") or "1536")
    ensure_index(index_name, dimension=dim)
    host = _private_host(pc, index_name)
    index = pc.Index(host=host)
    B = 100
    for i in range(0, len(vectors), B):
        index.upsert(vectors[i:i + B])
# ======================================================================


# ======================= Confluence + Coveo ===========================
# Helpers from your teammate’s utilities
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


# Coveo
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
# ======================================================================


# =========================== Ingestion ================================
def ingest_confluence(sources: List[Dict[str, Any]],
                      username: str,
                      api_token: str,
                      include_attachments: bool = True,
                      max_space_pages: int = 1000) -> List[Dict[str, Any]]:
    """
    sources: list of dicts like:
      - {"type":"root","url":"https://<site>/spaces/SPACEKEY/..."} -> fetch whole space (up to limit)
      - {"type":"page","url":"https://<site>/spaces/SPACE/pages/<id>"} -> fetch that page
    returns: [{"id": "...", "text":"...", "meta": {...}}]
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
                    text = d.page_content or ""
                    text = remove_repeated_newlines(text)
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


# -------- file readers (robust fallbacks) ----------
def read_pdf(path: str) -> List[Tuple[str, str, Dict[str, Any]]]:
    """Return list of (id, text, meta) page-wise."""
    pages: List[Tuple[str, str, Dict[str, Any]]] = []
    # 1) PyMuPDF
    try:
        import fitz  # PyMuPDF
        with fitz.open(path) as doc:
            for i, page in enumerate(doc):
                text = page.get_text("text") or page.get_text()
                pages.append((f"file:{path}#p{i+1}", text or "", {"source_type": "file", "filename": os.path.basename(path), "page": i+1}))
        return pages
    except Exception:
        pass
    # 2) PyPDF2
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
    # 3) pdfminer (last resort, single blob)
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
    # Try textract (if installed)
    try:
        import textract  # type: ignore
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
        # flatten values
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
        elif ext in (".docx",):
            text = read_docx(p)
            docs.append({"id": f"file:{p}", "text": text, "meta": {"source_type": "file", "filename": os.path.basename(p)}})
        elif ext in (".doc",):
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
# ======================================================================


# ======================= Chunking / Utilities =========================
from langchain.text_splitter import RecursiveCharacterTextSplitter

def _adaptive_splitter(meta: Dict[str, Any]) -> RecursiveCharacterTextSplitter:
    st = meta.get("source_type")
    if st == "confluence":
        # prose/KB pages
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
    # default
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
# ======================================================================


# ============================== Runner ===============================
def build_report(docs: List[Dict[str, Any]], chunks: List[Dict[str, Any]]) -> Dict[str, Any]:
    # words per doc and chunk counts
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


def store_text_corpus(index_name: str, corpus: List[Dict[str, Any]]) -> Dict[str, Any]:
    # 1) chunk
    chunks = chunk_documents(corpus)

    # 2) embed + upsert
    vectors: List[Tuple[str, List[float], Dict[str, Any]]] = []
    B = 64
    for i in range(0, len(chunks), B):
        batch = chunks[i:i + B]
        texts = [c["text"] for c in batch]
        embs = embed_texts(texts)
        for c, e in zip(batch, embs):
            vectors.append((c["id"], e, c["meta"]))
    upsert_embeddings(index_name, vectors)

    report = build_report(corpus, chunks)
    report.update({"index": index_name, "upserted": len(vectors)})
    return report


def run_pipeline(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    cfg example:
    {
      "team_name": "IAM",
      "index_name": "test-iam",            # optional; default = f"test-{team}"
      "confluence": {
        "username": "...",
        "api_token": "...",
        "sources": [
          {"type":"root","url":"https://xxx.atlassian.net/wiki/spaces/IAM/overview"},
          {"type":"page","url":"https://xxx.atlassian.net/wiki/spaces/IAM/pages/123456789"}
        ],
        "include_attachments": true,
        "max_space_pages": 500
      },
      "coveo": {
        "enabled": true,
        "organization_id": "...",
        "auth_token": "...",
        "user_email": "user@company.com",
        "tags": ["mylabel"]
      },
      "files": ["./docs/A.pdf","./docs/B.docx","./docs/table.xlsx"]
    }
    """
    team = (cfg.get("team_name") or "team").strip().lower().replace(" ", "-")
    index_name = (cfg.get("index_name") or f"test-{team}").lower()

    all_docs: List[Dict[str, Any]] = []

    # 1) Coveo discovery (optional) -> extra Confluence pages
    coveo_conf = cfg.get("coveo") or {}
    discovered_urls: List[str] = []
    if coveo_conf.get("enabled"):
        discovered_urls = discover_with_coveo(coveo_conf)
        if discovered_urls:
            # Append discovered as page sources
            conf = cfg.get("confluence") or {}
            srcs = conf.get("sources") or []
            for u in discovered_urls:
                srcs.append({"type": "page", "url": u})
            conf["sources"] = srcs
            cfg["confluence"] = conf

    # 2) Confluence ingestion
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

    # 3) File ingestion
    files = cfg.get("files") or []
    if files:
        docs_f = ingest_files(files)
        all_docs.extend(docs_f)

    # 4) Store -> Pinecone
    report = store_text_corpus(index_name, all_docs)
    # Add a quick top-level summary
    report["discovered_urls"] = discovered_urls
    return report
# ======================================================================


# =============================== CLI =================================
def _read_json_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    if len(sys.argv) < 2:
        print("Usage: python pipeline.py <config.json>")
        print("Minimal example config:\n")
        example = {
            "team_name": "IAM",
            # "index_name": "test-iam",  # optional
            "confluence": {
                "username": "user@company.com",
                "api_token": "CONFLUENCE_API_TOKEN",
                "sources": [
                    {"type":"root","url":"https://your.atlassian.net/wiki/spaces/IAM/overview"},
                    {"type":"page","url":"https://your.atlassian.net/wiki/spaces/IAM/pages/123456789/Page-Title"}
                ],
                "include_attachments": True,
                "max_space_pages": 300
            },
            "coveo": {
                "enabled": False,
                "organization_id": "your-org-id",
                "auth_token": "COVEO_PLATFORM_TOKEN",
                "user_email": "user@company.com",
                "tags": ["samplelabel"]
            },
            "files": ["./sample.pdf","./readme.md","./sheet.xlsx"]
        }
        print(json.dumps(example, indent=2))
        sys.exit(1)

    cfg = _read_json_file(sys.argv[1])
    try:
        report = run_pipeline(cfg)
        print(json.dumps(report, indent=2))
    except Exception as e:
        print("[ERROR] Pipeline failed:")
        traceback.print_exc()
        sys.exit(2)


if __name__ == "__main__":
    main()
