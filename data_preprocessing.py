#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
processor_job.py — Single-file Confluence processor with Coveo + optional descendants
Reads the Step-2 sources JSON from S3, extracts Confluence URL(s) and optional tags,
optionally expands to descendant sub-pages, uses Coveo to discover more pages by tag,
downloads content, diffs vs last run, and writes outputs back to S3.

USAGE
=====
python processor_job.py \
  --bucket <S3_BUCKET> \
  --sources-key coach/teams/<email>/sources/<timestamp>.json \
  --team-email <email@company.com> \
  --output-prefix coach/teams \
  --region us-east-1 \
  [--aws-profile your-profile] \
  [--expand auto|on|off] \
  [--max-pages 2000]

ENV (required)
==============
CONFLUENCE_USERNAME=you@company.com
CONFLUENCE_API_KEY=<personal-access-token>

Coveo (optional; enable if provided)
COVEO_ORG_ID=<org>
COVEO_PLATFORM_TOKEN=<platform_api_token>
COVEO_USER_EMAIL=you@company.com

TLS / corporate proxy (optional):
REQUESTS_CA_BUNDLE=/abs/path/to/your/corp-root.pem  (or AWS_CA_BUNDLE)

Deps:
  pip install boto3 requests beautifulsoup4 langchain-community certifi
"""

from __future__ import annotations

import os, io, re, sys, json, argparse, hashlib, logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import boto3
from botocore.config import Config as BotoConfig
import requests
from requests.auth import HTTPBasicAuth

try:
    import certifi
    CERT_PATH = certifi.where()
except Exception:
    CERT_PATH = None

# LangChain community Confluence loader
from langchain_community.document_loaders import ConfluenceLoader

# ----------------------------- logging -----------------------------
log = logging.getLogger("processor_job")
if not log.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(h)
log.setLevel(logging.INFO)

# ----------------------------- helpers -----------------------------
def now_ts() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%S")

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def key_join(*parts: str) -> str:
    return "/".join(p.strip("/") for p in parts if p)

def normalize_bucket_name(raw: str) -> str:
    if not raw: return raw
    if raw.startswith("s3://"): raw = raw[5:]
    return raw.split("/", 1)[0]

def remove_repeated_newlines_text(s: str) -> str:
    return re.sub(r"\n(?:[\t ]*\n)+", "\n\n", s)

class PageIDNotFoundError(Exception): ...
class BaseURLNotFoundError(Exception): ...

def get_base_url(url: str) -> str:
    """Extract https://<host>/wiki from a Confluence page URL."""
    m = re.match(r"^(https?://[^/]+/wiki)", url)
    if m: return m.group(1)
    raise BaseURLNotFoundError("Base URL not matched (expected .../wiki/...)")

def get_page_id(url: str) -> str:
    """Extract numeric page id from /pages/<ID> in URL."""
    m = re.search(r"/pages/(\d+)", url)
    if m: return m.group(1)
    raise PageIDNotFoundError("No /pages/<id> segment found in URL")

# --------------------- Confluence descendant search ---------------------
def list_descendant_page_ids(
    base_url: str,
    root_page_id: str,
    username: str,
    api_token: str,
    verify: Any = True,
    limit: int = 200,
    max_pages: int = 2000,
) -> List[str]:
    """
    Returns ALL descendant page ids under the given root page.
    Uses CQL: ancestor=<root_id> AND type=page with pagination.
    """
    ids: List[str] = []
    start = 0
    auth = HTTPBasicAuth(username, api_token)
    search_url = f"{base_url}/rest/api/search"

    while True:
        cql = f"ancestor={root_page_id} AND type=page"
        params = {"cql": cql, "limit": str(limit), "start": str(start)}
        r = requests.get(search_url, params=params, auth=auth, timeout=45, verify=verify)
        r.raise_for_status()
        data = r.json() or {}
        results = data.get("results", [])
        for item in results:
            cid = None
            if isinstance(item, dict):
                content = item.get("content")
                if isinstance(content, dict):
                    cid = content.get("id")
                if not cid:
                    cid = item.get("id")
            if cid:
                ids.append(str(cid))

        size = data.get("size", len(results))
        if size == 0 or len(ids) >= max_pages:
            break
        start += size

    # unique, preserve order
    seen = set(); ordered = []
    for i in ids:
        if i not in seen:
            ordered.append(i); seen.add(i)
    return ordered

# --------------------------- Coveo client ---------------------------
class CoveoSearch:
    """
    Minimal client to obtain a search token and query by tag (@conflabels=<tag>).
    """
    def __init__(self, organization_id: str, platform_token: str, verify=True):
        self.organization_id = organization_id
        self.platform_token = platform_token
        self.base_url = "https://platform.cloud.coveo.com/rest/search/v2"
        self.search_url = f"https://{organization_id}.org.coveo.com/rest/search/v2"
        self.verify = verify

    def get_token(self, user_email: str) -> str:
        url = f"{self.base_url}/token"
        payload = {
            "organizationId": self.organization_id,
            "validFor": 180000,  # ms
            "userIds": [{"name": user_email, "provider": "Email Security Provider"}],
        }
        headers = {
            "authorization": f"Bearer {self.platform_token}",
            "content-type": "application/json",
        }
        r = requests.post(url, json=payload, headers=headers, timeout=30, verify=self.verify)
        r.raise_for_status()
        return r.json().get("token", "")

    def search_links(self, tag: str, token: str) -> List[str]:
        qs = {"organizationId": self.organization_id}
        payload = {"q": f"@conflabels={tag}"}
        headers = {
            "authorization": f"Bearer {token}",
            "content-type": "application/json",
        }
        r = requests.post(self.search_url, json=payload, headers=headers, params=qs, timeout=30, verify=self.verify)
        r.raise_for_status()
        res = r.json() or {}
        return [row.get("clickUri") for row in res.get("results", []) if row.get("clickUri")]

# --------------------------- core fetcher ---------------------------
class ContentFetcher:
    def __init__(self, username: str, api_token: str):
        self.username = username
        self.api_token = api_token

    def fetch_by_urls(self, urls: List[str]) -> Dict[str, str]:
        """Fetch each URL individually via loader (uses page_ids extracted from URL)."""
        out: Dict[str, str] = {}
        for i, url in enumerate(urls, 1):
            try:
                pid = get_page_id(url)
                base = get_base_url(url)
                text = self._load_pages(base=base, page_ids=[pid])
                out[url] = text[0] if text else "Empty page"
                log.info("Fetched %s/%s (id=%s)", i, len(urls), pid)
            except Exception as e:
                log.exception("Failed fetching %s", url)
                out[url] = f"Exception: {e}"
        return out

    def fetch_by_ids(self, base_url: str, page_ids: List[str]) -> Dict[str, str]:
        """Fetch a batch of page ids under the same base_url."""
        out: Dict[str, str] = {}
        if not page_ids:
            return out
        texts = self._load_pages(base=base_url, page_ids=page_ids)
        for pid, txt in zip(page_ids, texts):
            url_key = f"{base_url}/pages/{pid}"
            out[url_key] = txt or "Empty page"
        return out

    def _load_pages(self, base: str, page_ids: List[str]) -> List[str]:
        loader = ConfluenceLoader(
            url=base,
            username=self.username,
            api_key=self.api_token,
            page_ids=page_ids,
            include_attachments=True,
        )
        docs = loader.load()
        texts: List[str] = []
        for d in docs:
            title = (d.metadata or {}).get("title", "")
            body = d.page_content or ""
            texts.append(remove_repeated_newlines_text(f"{title}:\n{body}"))
        return texts

# ----------------------------- processor -----------------------------
@dataclass
class Inputs:
    bucket: str
    sources_key: str           # S3 key to Step-2 knowledge JSON (contains Confluence url(s)/tags)
    team_email: str
    output_prefix: str         # e.g., "coach/teams"
    region: str = "us-east-1"
    aws_profile: Optional[str] = None
    expand: str = "auto"       # "auto" (default), "on", "off"
    max_pages: int = 2000

class Processor:
    def __init__(self):
        self.verify_requests = os.getenv("AWS_CA_BUNDLE") or os.getenv("REQUESTS_CA_BUNDLE") or (CERT_PATH or True)

    def _session(self, region: str, profile: Optional[str]) -> boto3.Session:
        return boto3.Session(profile_name=profile, region_name=region) if profile else boto3.Session(region_name=region)

    def _s3(self, sess: boto3.Session):
        return sess.client("s3", config=BotoConfig(retries={"max_attempts": 5, "mode": "standard"}))

    def s3_get_json(self, s3, bucket: str, key: str) -> Any:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.load(io.BytesIO(obj["Body"].read()))

    def s3_put_json(self, s3, bucket: str, key: str, payload: Any) -> None:
        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(payload, indent=2).encode("utf-8"), ContentType="application/json")

    # ----- Extract Confluence URLs and optional Coveo tags from saved JSON -----
    def _extract_confluence_urls_and_tags(self, src: Any) -> tuple[list[str], list[str]]:
        urls: List[str] = []
        tags: List[str] = []

        def push_url(u):
            if isinstance(u, str) and u.strip():
                urls.append(u.strip())

        def push_tag(t):
            if isinstance(t, str) and t.strip():
                tags.append(t.strip())

        if isinstance(src, dict):
            cf = src.get("confluence") or src.get("Confluence")
            if isinstance(cf, dict):
                if cf.get("url"):  push_url(cf["url"])
                if isinstance(cf.get("urls"), list):
                    for u in cf["urls"]: push_url(u)
                # tags can be string or array
                if cf.get("tags"):
                    if isinstance(cf["tags"], list):
                        for t in cf["tags"]: push_tag(t)
                    else:
                        push_tag(str(cf["tags"]))

            if isinstance(src.get("sources"), list):
                for item in src["sources"]:
                    if not isinstance(item, dict): continue
                    kind = (item.get("kind") or item.get("type") or "").lower()
                    if kind == "confluence":
                        if item.get("url"): push_url(item["url"])
                        if isinstance(item.get("urls"), list):
                            for u in item["urls"]: push_url(u)
                        if item.get("tags"):
                            if isinstance(item["tags"], list):
                                for t in item["tags"]: push_tag(t)
                            else:
                                push_tag(str(item["tags"]))

            if src.get("url"): push_url(src["url"])

        # unique, preserve order
        def unique_keep_order(seq: List[str]) -> List[str]:
            seen = set(); out = []
            for x in seq:
                if x not in seen:
                    out.append(x); seen.add(x)
            return out

        return unique_keep_order(urls), unique_keep_order(tags)

    def run(self, p: Inputs) -> Dict[str, Any]:
        bucket = normalize_bucket_name(p.bucket)
        ts = now_ts()
        job_prefix    = key_join(p.output_prefix, p.team_email, "jobs", ts)
        latest_prefix = key_join(p.output_prefix, p.team_email, "latest")

        sess = self._session(p.region, p.aws_profile)
        s3   = self._s3(sess)

        # Load sources JSON saved by UI
        sources = self.s3_get_json(s3, bucket, p.sources_key)

        # Confluence creds
        conf_user = os.getenv("CONFLUENCE_USERNAME") or ""
        conf_key  = os.getenv("CONFLUENCE_API_KEY") or ""
        if not (conf_user and conf_key):
            raise RuntimeError("CONFLUENCE_USERNAME / CONFLUENCE_API_KEY env vars are required")

        urls, tags = self._extract_confluence_urls_and_tags(sources)
        if not urls and not tags:
            raise RuntimeError("No Confluence URLs or Coveo tags found in sources JSON")

        # Optional Coveo expansion (by tag)
        cov_org  = os.getenv("COVEO_ORG_ID") or ""
        cov_tok  = os.getenv("COVEO_PLATFORM_TOKEN") or ""
        cov_user = os.getenv("COVEO_USER_EMAIL") or ""
        discovered_by_coveo: List[str] = []
        if cov_org and cov_tok and cov_user and tags:
            try:
                coveo = CoveoSearch(cov_org, cov_tok, verify=self.verify_requests)
                token = coveo.get_token(cov_user)
                for t in tags:
                    try:
                        found = coveo.search_links(t, token)
                        discovered_by_coveo.extend(found)
                    except Exception as e:
                        log.warning("Coveo search failed for tag '%s': %s", t, e)
                log.info("Coveo discovered %d URLs via tags", len(discovered_by_coveo))
            except Exception as e:
                log.warning("Coveo disabled (token error): %s", e)

        # Combine: direct URLs + Coveo-discovered
        all_urls: List[str] = []
        seen = set()
        for u in list(urls) + list(discovered_by_coveo):
            if isinstance(u, str) and u.strip() and u not in seen:
                all_urls.append(u); seen.add(u)

        if not all_urls:
            raise RuntimeError("No Confluence URLs to fetch after Coveo expansion")

        fetcher = ContentFetcher(conf_user, conf_key)

        # Expansion logic for descendants
        expand = (p.expand or "auto").lower()
        fetched: Dict[str, str] = {}

        if expand == "off" or (expand == "auto" and len(all_urls) > 1):
            # No descendant expansion (or multiple roots)
            log.info("Fetching %d URLs without descendant expansion", len(all_urls))
            fetched = fetcher.fetch_by_urls(all_urls)
        else:
            # Expand from the FIRST URL (treat it as project root)
            root_url = all_urls[0]
            root_id  = get_page_id(root_url)
            base     = get_base_url(root_url)
            log.info("Expanding descendants of %s (id=%s)", root_url, root_id)
            child_ids = list_descendant_page_ids(
                base_url=base,
                root_page_id=root_id,
                username=conf_user,
                api_token=conf_key,
                verify=self.verify_requests,
                limit=200,
                max_pages=p.max_pages,
            )
            all_ids = [root_id] + [i for i in child_ids if i != root_id]
            log.info("Total pages to fetch (root + descendants): %d", len(all_ids))
            fetched = fetcher.fetch_by_ids(base_url=base, page_ids=all_ids)

            # If Coveo discovered additional pages across other spaces, fetch those too (without expansion)
            extras = [u for u in all_urls[1:] if u not in fetched]
            if extras:
                log.info("Also fetching %d additional Coveo/direct URLs outside the root space", len(extras))
                fetched.update(fetcher.fetch_by_urls(extras))

        # Load previous fingerprints/output (if any)
        prev_fp_key = key_join(p.output_prefix, p.team_email, "latest", "fingerprints.json")
        prev_out_key= key_join(p.output_prefix, p.team_email, "latest", "confluence_output.json")
        try:
            prev_fp = self.s3_get_json(s3, bucket, prev_fp_key)
        except Exception:
            prev_fp = {}
        try:
            prev_out = self.s3_get_json(s3, bucket, prev_out_key)
        except Exception:
            prev_out = {}

        # Compute updates + new fingerprints
        updated: Dict[str, str] = {}
        new_fp: Dict[str, str] = {}
        for url, text in fetched.items():
            if not isinstance(text, str): continue
            h = sha1(text)
            new_fp[url] = h
            if prev_fp.get(url) != h:
                updated[url] = text

        # Write job artifacts
        out_key   = key_join(job_prefix, "confluence_output.json")
        fp_key    = key_join(job_prefix, "fingerprints.json")
        state_key = key_join(job_prefix, "state.json")

        self.s3_put_json(s3, bucket, out_key,   updated if updated else {})
        self.s3_put_json(s3, bucket, fp_key,    new_fp)
        self.s3_put_json(s3, bucket, state_key, {
            "root_urls": urls,
            "coveo_tags": tags,
            "coveo_urls": len(discovered_by_coveo),
            "expanded": expand != "off",
            "urls_total": len(fetched),
            "changed": len(updated),
            "timestamp": now_ts(),
            "sources_key": p.sources_key,
        })

        # Update "latest"
        self.s3_put_json(s3, bucket, key_join(latest_prefix, "confluence_output.json"), updated if updated else {})
        self.s3_put_json(s3, bucket, key_join(latest_prefix, "fingerprints.json"), new_fp)

        summary = {
            "job_prefix": job_prefix,
            "expanded": expand != "off",
            "urls_total": len(fetched),
            "changed": len(updated),
            "output_keys": {
                "job_output": out_key,
                "job_fingerprints": fp_key,
                "job_state": state_key,
                "latest_output": key_join(latest_prefix, "confluence_output.json"),
                "latest_fingerprints": key_join(latest_prefix, "fingerprints.json"),
            },
        }
        return summary

# ------------------------------ CLI -------------------------------
def parse_args(argv: List[str]) -> Inputs:
    ap = argparse.ArgumentParser(description="Confluence processor with Coveo + optional descendant expansion")
    ap.add_argument("--bucket", required=True, help="S3 bucket (name only)")
    ap.add_argument("--sources-key", required=True, help="S3 key of Step-2 sources JSON saved by UI")
    ap.add_argument("--team-email", required=True, help="Team contact email (used in output path)")
    ap.add_argument("--output-prefix", required=True, help="e.g., coach/teams")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--aws-profile", default=None)
    ap.add_argument("--expand", default="auto", choices=["auto", "on", "off"], help="auto (default): expand if one URL, on: always expand first URL, off: never")
    ap.add_argument("--max-pages", type=int, default=2000, help="cap total descendant pages")
    args = ap.parse_args(argv)
    return Inputs(
        bucket=args.bucket,
        sources_key=args.sources_key,
        team_email=args.team_email,
        output_prefix=args.output_prefix,
        region=args.region,
        aws_profile=args.aws_profile,
        expand=args.expand,
        max_pages=args.max_pages,
    )

def main(argv: List[str]) -> int:
    try:
        p = parse_args(argv)
        proc = Processor()
        summary = proc.run(p)
        print(json.dumps({"ok": True, "summary": summary}, indent=2))
        return 0
    except Exception as e:
        log.exception("Processing failed")
        print(json.dumps({"ok": False, "error": str(e)}), file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
