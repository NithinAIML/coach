# server/main.py
import os, json, uuid, hashlib, mimetypes
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Literal

import boto3
from botocore.client import Config as BotoConfig
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr

# ---------------- Env / Config ----------------
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET     = os.getenv("COACH_BUCKET")  # required
PREFIX     = os.getenv("COACH_PREFIX", "coach/")
ALLOWED_ORIGINS = os.getenv("FRONTEND_ORIGINS", "*")

if not BUCKET:
  raise RuntimeError("COACH_BUCKET env var is required")

s3 = boto3.client("s3", region_name=AWS_REGION, config=BotoConfig(signature_version="s3v4"))

def now_iso() -> str:
  return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def email_hash(email: str) -> str:
  return hashlib.sha256(email.strip().lower().encode("utf-8")).hexdigest()

def key_join(*parts: str) -> str:
  p = "/".join(p.strip("/").replace("//", "/") for p in parts if p is not None)
  return p + ("" if p.endswith("/") else "")

def s3_put_bytes(key: str, data: bytes, content_type: str):
  try:
    s3.put_object(Bucket=BUCKET, Key=key, Body=data, ContentType=content_type)
  except Exception as e:
    raise HTTPException(status_code=500, detail=f"S3 put failed: {e}")

def s3_head(key: str) -> bool:
  try:
    s3.head_object(Bucket=BUCKET, Key=key)
    return True
  except s3.exceptions.NoSuchKey:
    return False
  except Exception:
    return False

def list_prefix(prefix: str) -> bool:
  try:
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1)
    return resp.get("KeyCount", 0) > 0
  except Exception as e:
    raise HTTPException(status_code=500, detail=f"S3 list failed: {e}")

# ---------------- Models ----------------
class FileMeta(BaseModel):
  name: str
  type: Optional[str] = None
  size: Optional[int] = None

class PresignIn(BaseModel):
  contactEmail: EmailStr
  files: List[FileMeta]

class PresignedOut(BaseModel):
  name: str
  key: str
  url: str
  headers: Dict[str, str]

# ---------------- App ----------------
app = FastAPI(title="COACH backend")

origins = ["*"] if ALLOWED_ORIGINS.strip() == "*" else [o.strip() for o in ALLOWED_ORIGINS.split(",") if o.strip()]
app.add_middleware(
  CORSMiddleware,
  allow_origins=origins,
  allow_credentials=True,
  allow_methods=["*"],
  allow_headers=["*"],
)

# ---------------- Routes ----------------

@app.get("/api/health")
def health():
  return {"ok": True, "time": now_iso()}

@app.get("/api/registration-status")
def registration_status(email: EmailStr = Query(..., description="contactEmail used during registration")):
  eh = email_hash(email)
  pref = key_join(PREFIX, "registration", eh) + "/"
  exists = list_prefix(pref)
  return {"registered": bool(exists)}

@app.post("/api/presign-files", response_model=List[PresignedOut])
def presign_files(payload: PresignIn):
  outs: List[PresignedOut] = []
  for f in payload.files:
    base, ext = os.path.splitext(f.name)
    ext = ext.lower()
    if ext not in {".pdf", ".doc", ".docx", ".txt", ".md", ".json", ".xml"}:
      raise HTTPException(status_code=400, detail=f"File type not allowed: {ext}")
    eh = email_hash(payload.contactEmail)
    today = datetime.now(timezone.utc).date().isoformat()
    safe_base = "".join(c for c in (base or "file") if (c.isalnum() or c in "-_")) or "file"
    key = key_join(PREFIX, "uploads", eh, today, f"{safe_base}-{uuid.uuid4().hex}{ext}")
    content_type = f.type or (mimetypes.guess_type(f.name)[0] or "application/octet-stream")
    try:
      url = s3.generate_presigned_url(
        ClientMethod="put_object",
        Params={"Bucket": BUCKET, "Key": key, "ContentType": content_type},
        ExpiresIn=3600,
      )
    except Exception as e:
      raise HTTPException(status_code=500, detail=f"Presign failed: {e}")
    outs.append(PresignedOut(name=f.name, key=key, url=url, headers={"Content-Type": content_type}))
  return outs

@app.post("/api/put")
async def api_put(req: Request):
  """
  Store JSON to S3.
  Body: { key?: string, payload: any }
  If key omitted and payload.kind provided:
    - "registration" + contactEmail -> coach/registration/<hash>/<ts>.json
    - "sources" + contactEmail      -> coach/sources/<hash>/<ts>.json
    - otherwise                     -> coach/misc/<uuid>.json
  """
  try:
    body = await req.json()
  except Exception:
    raise HTTPException(status_code=400, detail="Invalid JSON body")
  key = body.get("key")
  payload = body.get("payload")
  if payload is None:
    raise HTTPException(status_code=400, detail="Missing 'payload'")

  if not key:
    kind = payload.get("kind")
    email = payload.get("contactEmail")
    ts = now_iso()
    if kind == "registration" and email:
      key = key_join(PREFIX, "registration", email_hash(email), f"{ts}.json")
    elif kind == "sources" and email:
      key = key_join(PREFIX, "sources", email_hash(email), f"{ts}.json")
    else:
      key = key_join(PREFIX, "misc", f"{uuid.uuid4().hex}.json")

  s3_put_bytes(key, json.dumps(payload, ensure_ascii=False).encode("utf-8"), "application/json")
  return {"ok": True, "key": key}

@app.post("/api/s3")
async def s3_exists(req: Request):
  """
  Check if a key exists. Accepts either { "key": "<key>" } or raw text body containing the key.
  Returns 200 if exists, 404 otherwise.
  """
  key = None
  # Try JSON first
  try:
    body = await req.json()
    key = body.get("key")
  except Exception:
    pass
  # Fallback: plain text
  if not key:
    try:
      key = (await req.body()).decode("utf-8").strip()
    except Exception:
      pass

  if not key:
    raise HTTPException(status_code=400, detail="Missing key")

  if s3_head(key):
    return {"ok": True, "key": key}
  else:
    raise HTTPException(status_code=404, detail="Not found")
