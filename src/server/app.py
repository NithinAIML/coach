# app.py
# FastAPI backend for S3:
# - POST /api/put     : store JSON payload to S3 (like putS3)
# - POST /api/s3      : check if an object exists (like checkS3)
# - POST /api/upload  : upload files (pdf/xlsx/docx/xml/json/...) as-is to S3 (like uploadFileToS3)
# - GET  /health      : basic health check
#
# Env (set in your runtime or .env):
#   AWS_REGION=us-east-1
#   AWS_ACCESS_KEY_ID=...
#   AWS_SECRET_ACCESS_KEY=...
#   BUCKET=prod-bucket
#   DeploymentRuntime=prod            # optional; if set, bucket becomes "<DeploymentRuntime>-bucket"
#   S3_SSE=AES256                     # optional; server-side encryption
#
# Install:
#   pip install fastapi uvicorn boto3 pydantic
#
# Run:
#   uvicorn app:app --reload

import io
import json
import os
from typing import Any, Dict, Optional, Union

import boto3
from botocore.exceptions import ClientError
from fastapi import (
    FastAPI,
    Body,
    File,
    Form,
    UploadFile,
    Request,
    Response,
    status,
)
from fastapi.middleware.cors import CORSMiddleware

# ------------- Config -------------

DEFAULT_REGION = os.getenv("AWS_REGION", "us-east-1")
DEFAULT_BUCKET = os.getenv("BUCKET", "prod-bucket")
DEPLOYMENT_RUNTIME = os.getenv("DeploymentRuntime") or os.getenv("DEPLOYMENT_RUNTIME")

# If DEPLOYMENT_RUNTIME is provided, follow the "<runtime>-bucket" rule
if DEPLOYMENT_RUNTIME:
    BUCKET_NAME = f"{DEPLOYMENT_RUNTIME}-bucket"
else:
    BUCKET_NAME = DEFAULT_BUCKET

S3_SSE = os.getenv("S3_SSE")  # e.g., "AES256"
DEFAULT_KEY = "chat-rfp/endpoint/inference/input/input.json"

# ------------- AWS Clients -------------

s3 = boto3.client("s3", region_name=DEFAULT_REGION)

# ------------- FastAPI App -------------

app = FastAPI(title="S3 Bridge API", version="1.0.0")

# CORS (adjust allow_origins for your domains in prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten this in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------- Helpers -------------

def _put_json(bucket: str, key: str, payload: Union[Dict[str, Any], Any]) -> Dict[str, Any]:
    """Store JSON payload to S3 at bucket/key."""
    body_bytes = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    args = {
        "Bucket": bucket,
        "Key": key,
        "Body": body_bytes,
        "ContentType": "application/json",
    }
    if S3_SSE:
        args["ServerSideEncryption"] = S3_SSE

    result = s3.put_object(**args)
    return {
        "bucket": bucket,
        "key": key,
        "etag": result.get("ETag"),
        "server_side_encryption": result.get("ServerSideEncryption"),
    }

def _head_exists(bucket: str, key: str) -> bool:
    """Return True if S3 object exists at bucket/key."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        # Any other error should bubble up
        raise

def _upload_stream(bucket: str, key: str, file_obj, content_type: Optional[str]) -> Dict[str, Any]:
    """Upload a file-like object as-is to S3."""
    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type
    if S3_SSE:
        extra_args["ServerSideEncryption"] = S3_SSE

    s3.upload_fileobj(file_obj, bucket, key, ExtraArgs=extra_args or None)

    # Try to fetch ETag for convenience (optional)
    etag = None
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        etag = head.get("ETag")
    except Exception:
        pass

    return {"bucket": bucket, "key": key, "etag": etag, "content_type": content_type}

# ------------- Routes -------------

@app.get("/health")
def health():
    return {"ok": True, "bucket": BUCKET_NAME, "region": DEFAULT_REGION}

@app.post("/api/put")
async def api_put(
    request: Request,
    key: Optional[str] = None,
):
    """
    Save a JSON payload to S3.
    - Query param ?key=... (optional)
    - If no key, uses DEFAULT_KEY.
    """
    try:
        # Parse JSON (robust to raw text)
        try:
            payload = await request.json()
        except Exception:
            raw = await request.body()
            try:
                payload = json.loads(raw.decode("utf-8"))
            except Exception:
                return Response(
                    content=json.dumps({"error": "Invalid JSON payload"}),
                    media_type="application/json",
                    status_code=status.HTTP_400_BAD_REQUEST,
                )

        final_key = key or DEFAULT_KEY
        out = _put_json(BUCKET_NAME, final_key, payload)
        return out

    except ClientError as e:
        return Response(
            content=json.dumps({"error": "S3 client error", "detail": str(e)}),
            media_type="application/json",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    except Exception as e:
        return Response(
            content=json.dumps({"error": "Unhandled error", "detail": str(e)}),
            media_type="application/json",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

@app.post("/api/s3")
async def api_check_s3(
    request: Request,
    key: Optional[str] = None,
):
    """
    Check if an S3 object exists.
    Accepts any of:
      - Query:     ?key=path/to/object
      - JSON body: {"key":"path/to/object"}
      - Raw text:  "path/to/object"
    Returns 200 if exists, 404 if not.
    """
    try:
        body_key: Optional[str] = None

        # Prefer JSON
        try:
            data = await request.json()
            if isinstance(data, dict) and isinstance(data.get("key"), str):
                body_key = data["key"]
        except Exception:
            # Fallback: raw text
            raw = (await request.body()).decode("utf-8").strip()
            if raw and raw not in {"null", "undefined"}:
                if (raw.startswith('"') and raw.endswith('"')) or (raw.startswith("'") and raw.endswith("'")):
                    raw = raw[1:-1]
                body_key = raw

        final_key = key or body_key
        if not final_key:
            return Response(
                content=json.dumps({"error": "Missing key. Provide ?key=... or body {'key':'...'}"}),
                media_type="application/json",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        exists = _head_exists(BUCKET_NAME, final_key)
        if exists:
            return {"exists": True, "bucket": BUCKET_NAME, "key": final_key}
        else:
            return Response(
                content=json.dumps({"exists": False, "bucket": BUCKET_NAME, "key": final_key}),
                media_type="application/json",
                status_code=status.HTTP_404_NOT_FOUND,
            )

    except ClientError as e:
        return Response(
            content=json.dumps({"error": "S3 client error", "detail": str(e)}),
            media_type="application/json",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    except Exception as e:
        return Response(
            content=json.dumps({"error": "Unhandled error", "detail": str(e)}),
            media_type="application/json",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

@app.post("/api/upload")
async def api_upload(
    file: UploadFile = File(...),
    key: Optional[str] = Form(None),
):
    """
    Upload a binary file as-is to S3.
    Form fields (multipart/form-data):
      - file: the uploaded file
      - key : optional S3 key; if omitted -> "uploads/<filename>"
    """
    try:
        filename = file.filename or "upload.bin"
        final_key = key or f"uploads/{filename}"
        content_type = file.content_type or "application/octet-stream"

        # Wrap in buffer so we can be sure it's a file-like object
        # (UploadFile.file already is, but this keeps it consistent)
        # If you want to stream without buffering, use file.file directly.
        buffer = io.BytesIO(await file.read())
        buffer.seek(0)

        out = _upload_stream(BUCKET_NAME, final_key, buffer, content_type)
        return out

    except ClientError as e:
        return Response(
            content=json.dumps({"error": "S3 client error", "detail": str(e)}),
            media_type="application/json",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    except Exception as e:
        return Response(
            content=json.dumps({"error": "Unhandled error", "detail": str(e)}),
            media_type="application/json",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
