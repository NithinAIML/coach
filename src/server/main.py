import os, json, uuid, hashlib, mimetypes
from datetime import datetime, timezone
from typing import List, Optional, Literal, Dict, Any

import boto3
from botocore.client import Config as BotoConfig
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr

# ---------- Config ----------
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET     = os.getenv("COACH_BUCKET")  # REQUIRED
PREFIX     = os.getenv("COACH_PREFIX", "coach/")
ALLOWED_ORIGINS = os.getenv("FRONTEND_ORIGINS", "*")  # comma-separated or "*" for dev

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

def s3_put_json(key: str, data: Dict[str, Any]):
    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3 put failed: {e}")

def list_prefix(prefix: str) -> bool:
    try:
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1)
        return resp.get("KeyCount", 0) > 0
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3 list failed: {e}")

# ---------- Models ----------
class FormModel(BaseModel):
    teamName: str
    department: str
    domain: str
    contactEmail: EmailStr
    description: str = ""

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

class SourceConfluence(BaseModel):
    name: str = ""
    url: str = ""
    description: str = ""
    autoRefresh: bool = True
    frequency: Literal["Daily", "Weekly", "Monthly"] = "Weekly"
    time: str = "09:00"

class SourceFileUpload(BaseModel):
    name: str = ""
    description: str = ""
    autoRefresh: bool = True
    frequency: Literal["Daily", "Weekly", "Monthly"] = "Weekly"
    time: str = "09:00"
    filesMeta: List[FileMeta] = []
    filesS3: List[Dict[str, str]] = []

class SaveSourcesIn(BaseModel):
    contactEmail: EmailStr
    selected: List[str] = []
    confluence: SourceConfluence
    fileUpload: SourceFileUpload

# ---------- App ----------
app = FastAPI(title="COACH backend")

# CORS
origins = (
    ["*"] if ALLOWED_ORIGINS.strip() == "*"
    else [o.strip() for o in ALLOWED_ORIGINS.split(",") if o.strip()]
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Helpers ----------
ALLOWED_EXTS = {".pdf", ".doc", ".docx", ".txt", ".md", ".json", ".xml"}

def sanitize_key_component(s: str) -> str:
    return "".join(c for c in s if (c.isalnum() or c in "-_")).strip() or "file"

def make_upload_key(email: str, filename: str) -> str:
    base, ext = os.path.splitext(filename)
    if ext.lower() not in ALLOWED_EXTS:
        raise HTTPException(status_code=400, detail=f"File type not allowed: {ext}")
    today = datetime.now(timezone.utc).date().isoformat()
    return key_join(
        PREFIX,
        "uploads",
        email_hash(email),
        today,
        f"{sanitize_key_component(base)}-{uuid.uuid4().hex}{ext.lower()}",
    )

# ---------- Routes ----------

@app.get("/api/health")
def health():
    return {"ok": True, "time": now_iso()}

@app.post("/api/team-registration")
def team_registration(payload: FormModel):
    eh = email_hash(payload.contactEmail)
    ts = now_iso()
    key = key_join(PREFIX, "registration", eh, f"{ts}.json")
    s3_put_json(key, {**payload.model_dump(), "savedAt": ts})
    return {"ok": True, "key": key}

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
        key = make_upload_key(payload.contactEmail, f.name)
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

@app.post("/api/knowledge-sources")
def save_sources(payload: SaveSourcesIn):
    eh = email_hash(payload.contactEmail)
    ts = now_iso()
    key = key_join(PREFIX, "sources", eh, f"{ts}.json")
    data = payload.model_dump()
    data["savedAt"] = ts
    s3_put_json(key, data)
    return {"ok": True, "key": key}
