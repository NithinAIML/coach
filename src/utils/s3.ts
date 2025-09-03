// // frontend/src/utils/s3.ts

// export type PresignedItem = {
//   name: string;
//   key: string;
//   url: string;
//   headers: Record<string, string>;
// };

// async function jsonPost<T>(url: string, body?: unknown): Promise<T> {
//   const res = await fetch(url, {
//     method: 'POST',
//     headers: { 'Content-Type': 'application/json' },
//     body: body != null ? JSON.stringify(body) : undefined,
//   });
//   if (!res.ok) {
//     let msg = '';
//     try { msg = await res.text(); } catch {}
//     throw new Error(msg || `Request failed: ${res.status}`);
//   }
//   try { return await res.json(); } catch { return undefined as unknown as T; }
// }

// /**
//  * Write JSON to S3 via backend.
//  * - Your backend chooses the S3 key if you omit it.
//  * - If you pass payload.kind === "registration"/"sources" + contactEmail,
//  *   backend will store under coach/registration|sources/<hash>/<ts>.json
//  */
// export async function putS3(
//   payload: any,
//   key?: string
// ): Promise<{ ok: boolean; key: string }> {
//   return jsonPost<{ ok: boolean; key: string }>('/api/put', { key, payload });
// }

// /**
//  * Poll/check whether an exact S3 key exists (used by async flows).
//  * Returns true iff /api/s3 says the object exists (HTTP 200).
//  */
// export async function checkS3(key: string): Promise<boolean> {
//   const res = await fetch('/api/s3', {
//     method: 'POST',
//     headers: { 'Content-Type': 'application/json' },
//     body: JSON.stringify({ key }),
//   });
//   if (res.status === 200) return true;
//   if (res.status === 404) return false;
//   return false;
// }

// /**
//  * Ask backend for presigned PUT URLs for files.
//  * Use for Step-2 "File upload" to send the actual binary from the browser.
//  */
// export async function presignFiles(
//   contactEmail: string,
//   files: File[]
// ): Promise<PresignedItem[]> {
//   const req = {
//     contactEmail,
//     files: files.map((f) => ({
//       name: f.name,
//       type: (f as any).type || undefined,
//       size: f.size,
//     })),
//   };
//   return jsonPost<PresignedItem[]>('/api/presign-files', req);
// }

// /**
//  * Upload a single file to S3 using a presigned PUT URL.
//  */
// export async function uploadWithPresigned(item: PresignedItem, file: File): Promise<void> {
//   const res = await fetch(item.url, {
//     method: 'PUT',
//     headers: item.headers,
//     body: file,
//   });
//   if (!res.ok) throw new Error(`Upload failed for ${item.name}: ${res.status} ${res.statusText}`);
// }

// /**
//  * Optional: direct S3 'put' analog your teammate had (kept for API compatibility),
//  * but here it simply calls /api/put with a fixed key if provided.
//  */
// export async function s3put(payload: any, key?: string): Promise<{ ok: boolean; key: string }> {
//   return putS3(payload, key);
// }

// src/utils/s3.ts
// export type PresignedItem = {
//   name: string;
//   key: string;
//   url: string; // PUT URL
// };

// export async function putS3(payload: any): Promise<Response> {
//   if (!payload || !payload.kind) {
//     throw new Error("payload.kind is required");
//   }
//   if (!payload.teamEmail || typeof payload.teamEmail !== "string") {
//     throw new Error("teamEmail is required");
//   }
//   return fetch("/api/put", {
//     method: "POST",
//     headers: { "Content-Type": "application/json" },
//     body: JSON.stringify(payload),
//   });
// }

// /**
//  * Ask server for presigned PUT URLs for files.
//  * Returns [{ name, key, url }]
//  */
// export async function presignFiles(teamEmail: string, files: File[]): Promise<PresignedItem[]> {
//   if (!teamEmail) throw new Error("teamEmail is required");
//   const meta = files.map((f) => ({ name: f.name, type: (f as any).type || f.type || "application/octet-stream", size: f.size }));
//   const res = await fetch("/api/upload", {
//     method: "POST",
//     headers: { "Content-Type": "application/json" },
//     body: JSON.stringify({ teamEmail, files: meta }),
//   });
//   if (!res.ok) {
//     const err = await safeJson(res);
//     throw new Error(err?.error || "Failed to request presigned URLs");
//   }
//   const data = await res.json();
//   return data.urls as PresignedItem[];
// }

// /**
//  * Use the presigned PUT URL to upload a file directly to S3.
//  */
// export async function uploadWithPresigned(item: PresignedItem, file: File): Promise<void> {
//   const contentType = (file as any).type || file.type || "application/octet-stream";
//   const put = await fetch(item.url, {
//     method: "PUT",
//     headers: { "Content-Type": contentType },
//     body: file,
//   });
//   if (!put.ok) {
//     // Some S3 errors still respond 200 with XML — handle explicit !ok first.
//     const text = await put.text().catch(() => "");
//     throw new Error(`Upload failed for ${item.name}: ${put.status} ${put.statusText} ${text?.slice(0, 200)}`);
//   }
// }

// /* ------------------------- helpers ------------------------- */
// async function safeJson(r: Response) {
//   try { return await r.json(); } catch { return undefined; }
// }

// utils/s3.ts
// Browser-side helpers used by home.tsx

export type PresignedInfo = {
  name: string;
  key: string;
  url: string;
  headers?: Record<string, string>;
};

export async function putS3(payload: any): Promise<Response> {
  const res = await fetch('/api/put', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  return res;
}

export async function presignFiles(email: string, files: File[]): Promise<PresignedInfo[]> {
  if (!email) throw new Error('Email is required for upload');
  const body = {
    email,
    files: files.map((f) => ({
      name: f.name,
      type: (f as any).type || 'application/octet-stream',
      size: f.size,
    })),
  };
  const res = await fetch('/api/upload', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const msg = await res.text().catch(() => '');
    throw new Error(msg || 'Failed to get presigned URLs');
  }
  const data = await res.json();
  return (data?.urls || []) as PresignedInfo[];
}

export async function uploadWithPresigned(p: PresignedInfo, file: File): Promise<void> {
  const headers: Record<string, string> = {
    'Content-Type': (file as any).type || 'application/octet-stream',
    ...(p.headers || {}),
  };
  const res = await fetch(p.url, { method: 'PUT', headers, body: file });
  if (!res.ok) {
    const msg = await res.text().catch(() => '');
    throw new Error(msg || `Upload failed for ${p.name}`);
  }
}
