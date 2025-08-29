// src/utils/s3.ts
// Client & Server helpers for S3.
// - Browser safe:
//     putS3(payload, { key?, baseUrl?, headers? })
//     checkS3(successKey, failureKey, { timeoutMs?, intervalMs?, baseUrl?, headers? })
//     uploadFileToS3(file, { key?, baseUrl?, headers? })  <-- NEW (raw files as-is)
// - Server (Node):
//     s3PutServer(payload, key?, bucket?)
//
// Keep AWS creds off the browser. /api/put, /api/s3, /api/upload are handled by your backend.

import type { PutObjectCommandOutput } from "@aws-sdk/client-s3";

let S3ClientCtor: any;
let PutObjectCommandCtor: any;

type PollResult =
  | { state: "success"; key: string }
  | { state: "failed"; key: string }
  | { state: "timeout" }
  | { state: "error"; error: string };

export interface CheckS3Options {
  timeoutMs?: number;
  intervalMs?: number;
  baseUrl?: string;
  headers?: Record<string, string>;
}

export interface PutS3Options {
  key?: string;
  baseUrl?: string;
  headers?: Record<string, string>;
}

export interface UploadFileOptions {
  key?: string;               // e.g. "knowledge/uploads/myfile.pdf"
  baseUrl?: string;           // point to your API if not same-origin
  headers?: Record<string, string>; // auth headers, if any
}

function apiBase(baseUrl?: string) {
  return (baseUrl ?? "").replace(/\/+$/, "");
}

async function postJson(url: string, body: any, headers?: Record<string, string>) {
  const isString = typeof body === "string";
  const res = await fetch(url, {
    method: "POST",
    headers: {
      ...(isString ? { "Content-Type": "text/plain" } : { "Content-Type": "application/json" }),
      ...headers,
    },
    body: isString ? body : JSON.stringify(body),
  });
  return res;
}

/** Store a JSON payload via backend /api/put */
export async function putS3(payload: unknown, opts: PutS3Options = {}): Promise<Response> {
  const base = apiBase(opts.baseUrl);
  const url = opts.key ? `${base}/api/put?key=${encodeURIComponent(opts.key)}` : `${base}/api/put`;

  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...(opts.headers ?? {}) },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    let detail = "";
    try {
      const data = await res.json();
      detail = data?.error || JSON.stringify(data);
    } catch {
      detail = await res.text().catch(() => "");
    }
    throw new Error(`putS3 failed (${res.status}): ${detail}`);
  }
  return res;
}

/** Poll backend /api/s3 for a success/failure key */
export async function checkS3(
  successKey: string,
  failureKey: string,
  opts: CheckS3Options = {}
): Promise<PollResult> {
  const timeoutMs = opts.timeoutMs ?? 300_000;
  const intervalMs = opts.intervalMs ?? 1_000;
  const base = apiBase(opts.baseUrl);
  const start = Date.now();

  async function exists(key: string): Promise<boolean | "error"> {
    try {
      const res = await postJson(`${base}/api/s3`, { key }, opts.headers);
      if (res.ok) return true;
      if (res.status === 404) return false;
      return "error";
    } catch {
      return "error";
    }
  }

  while (Date.now() - start < timeoutMs) {
    const succ = await exists(successKey);
    if (succ === true) return { state: "success", key: successKey };
    if (succ === "error") return { state: "error", error: "API error while checking success key" };

    const fail = await exists(failureKey);
    if (fail === true) return { state: "failed", key: failureKey };
    if (fail === "error") return { state: "error", error: "API error while checking failure key" };

    await new Promise((r) => setTimeout(r, intervalMs));
  }
  return { state: "timeout" };
}

/** Resolve bucket name like teammate's code */
export function getBucketFromEnv(): string {
  const runtime = (typeof process !== "undefined" && process?.env?.DeploymentRuntime) || (typeof process !== "undefined" && process?.env?.DEPLOYMENT_RUNTIME);
  if (runtime && String(runtime).trim()) return `${runtime}-bucket`;
  return (typeof process !== "undefined" && process?.env?.BUCKET) || "prod-bucket";
}

/** Server-only direct S3 PutObject */
export async function s3PutServer(
  payload: unknown,
  key: string = "chat-rfp/endpoint/inference/input/input.json",
  bucket?: string
): Promise<PutObjectCommandOutput> {
  if (!S3ClientCtor || !PutObjectCommandCtor) {
    const mod = await import("@aws-sdk/client-s3");
    S3ClientCtor = mod.S3Client;
    PutObjectCommandCtor = mod.PutObjectCommand;
  }
  const region = (typeof process !== "undefined" && process.env.AWS_REGION) || "us-east-1";
  const finalBucket = bucket || getBucketFromEnv();
  const client = new S3ClientCtor({ region });
  const command = new PutObjectCommandCtor({
    Bucket: finalBucket,
    Key: key,
    Body: Buffer.from(JSON.stringify(payload)),
    ContentType: "application/json",
  });
  return await client.send(command);
}

/**
 * Upload a raw file (pdf, xlsx, docx, xml, json, etc.) exactly as-is to S3 via backend /api/upload.
 * The backend must accept multipart/form-data with fields:
 * - "file": the binary file
 * - "key": optional target key in S3 (if omitted, backend uses a default/prefix).
 */
export async function uploadFileToS3(file: File, opts: UploadFileOptions = {}): Promise<{ bucket: string; key: string }> {
  const base = apiBase(opts.baseUrl);
  const url = `${base}/api/upload`;
  const form = new FormData();
  form.append("file", file);
  if (opts.key) form.append("key", opts.key);

  const res = await fetch(url, {
    method: "POST",
    headers: { ...(opts.headers ?? {}) }, // do NOT set Content-Type here (let browser set the boundary)
    body: form,
  });

  if (!res.ok) {
    let detail = "";
    try { detail = (await res.json())?.error ?? ""; } catch { detail = await res.text().catch(() => ""); }
    throw new Error(`uploadFileToS3 failed (${res.status}): ${detail}`);
  }

  const data = await res.json();
  return { bucket: data.bucket, key: data.key };
}
