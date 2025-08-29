// frontend/src/utils/s3.ts

export type PresignedItem = {
  name: string;
  key: string;
  url: string;
  headers: Record<string, string>;
};

async function jsonPost<T>(url: string, body?: unknown): Promise<T> {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: body != null ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    let msg = '';
    try { msg = await res.text(); } catch {}
    throw new Error(msg || `Request failed: ${res.status}`);
  }
  try { return await res.json(); } catch { return undefined as unknown as T; }
}

/**
 * Write JSON to S3 via backend.
 * - Your backend chooses the S3 key if you omit it.
 * - If you pass payload.kind === "registration"/"sources" + contactEmail,
 *   backend will store under coach/registration|sources/<hash>/<ts>.json
 */
export async function putS3(
  payload: any,
  key?: string
): Promise<{ ok: boolean; key: string }> {
  return jsonPost<{ ok: boolean; key: string }>('/api/put', { key, payload });
}

/**
 * Poll/check whether an exact S3 key exists (used by async flows).
 * Returns true iff /api/s3 says the object exists (HTTP 200).
 */
export async function checkS3(key: string): Promise<boolean> {
  const res = await fetch('/api/s3', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ key }),
  });
  if (res.status === 200) return true;
  if (res.status === 404) return false;
  return false;
}

/**
 * Ask backend for presigned PUT URLs for files.
 * Use for Step-2 "File upload" to send the actual binary from the browser.
 */
export async function presignFiles(
  contactEmail: string,
  files: File[]
): Promise<PresignedItem[]> {
  const req = {
    contactEmail,
    files: files.map((f) => ({
      name: f.name,
      type: (f as any).type || undefined,
      size: f.size,
    })),
  };
  return jsonPost<PresignedItem[]>('/api/presign-files', req);
}

/**
 * Upload a single file to S3 using a presigned PUT URL.
 */
export async function uploadWithPresigned(item: PresignedItem, file: File): Promise<void> {
  const res = await fetch(item.url, {
    method: 'PUT',
    headers: item.headers,
    body: file,
  });
  if (!res.ok) throw new Error(`Upload failed for ${item.name}: ${res.status} ${res.statusText}`);
}

/**
 * Optional: direct S3 'put' analog your teammate had (kept for API compatibility),
 * but here it simply calls /api/put with a fixed key if provided.
 */
export async function s3put(payload: any, key?: string): Promise<{ ok: boolean; key: string }> {
  return putS3(payload, key);
}
