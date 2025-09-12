// // src/utils/s3.ts
// type PresignInput = { name: string; type?: string };

// function ensureEmailFields(payload: any) {
//   const email =
//     payload?.contactEmail ||
//     payload?.teamEmail ||
//     payload?.email ||
//     payload?.userEmail ||
//     '';

//   if (!email) return payload;

//   return {
//     ...payload,
//     contactEmail: email,
//     teamEmail: email,
//     email, // for /api/upload
//   };
// }

// export async function putS3(payload: any): Promise<Response> {
//   const body = JSON.stringify(ensureEmailFields(payload));
//   const res = await fetch('/api/put', {
//     method: 'POST',
//     headers: { 'Content-Type': 'application/json' },
//     body,
//   });
//   return res;
// }

// export async function presignFiles(
//   email: string,
//   files: File[] | PresignInput[]
// ): Promise<Array<{ name: string; key: string; url: string }>> {
//   const thin = Array.from(files).map((f: any) => ({
//     name: f.name,
//     type: f.type || 'application/octet-stream',
//   }));

//   const res = await fetch('/api/upload', {
//     method: 'POST',
//     headers: { 'Content-Type': 'application/json' },
//     body: JSON.stringify(ensureEmailFields({ email, files: thin })),
//   });

//   if (!res.ok) {
//     const err = await res.json().catch(() => ({}));
//     throw new Error(err?.error || 'Failed to create presigned URLs');
//   }

//   const data = await res.json();
//   return data.uploads || [];
// }

// export async function uploadWithPresigned(
//   presigned: { url: string; key: string; name: string },
//   file: File | Blob
// ) {
//   const put = await fetch(presigned.url, {
//     method: 'PUT',
//     headers: { 'Content-Type': (file as any).type || 'application/octet-stream' },
//     body: file,
//   });
//   if (!put.ok) {
//     const text = await put.text().catch(() => '');
//     throw new Error(`Upload failed for ${presigned.name}: ${text || put.statusText}`);
//   }
//   return { ok: true, key: presigned.key };
// }


type PresignInput = { name: string; type?: string };

function ensureEmailFields(payload: any) {
  const email = payload?.contactEmail || payload?.teamEmail || payload?.email || payload?.userEmail || '';
  if (!email) return payload;
  return { ...payload, contactEmail: email, teamEmail: email, email };
}

export async function putS3(payload: any): Promise<Response> {
  const body = JSON.stringify(ensureEmailFields(payload));
  return fetch('/api/put', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body });
}

export async function presignFiles(
  email: string,
  files: File[] | PresignInput[]
): Promise<Array<{ name: string; key: string; url: string }>> {
  const thin = Array.from(files).map((f: any) => ({
    name: f.name,
    type: f.type || 'application/octet-stream',
  }));

  const res = await fetch('/api/upload', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(ensureEmailFields({ email, files: thin })),
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err?.error || 'Failed to create presigned URLs');
  }

  const data = await res.json();
  // tolerate either shape to be safe
  return data.uploads || data.urls || [];
}

export async function uploadWithPresigned(
  presigned: { url: string; key: string; name: string },
  file: File | Blob
) {
  const put = await fetch(presigned.url, {
    method: 'PUT',
    headers: { 'Content-Type': (file as any).type || 'application/octet-stream' },
    body: file,
  });
  if (!put.ok) {
    const text = await put.text().catch(() => '');
    throw new Error(`Upload failed for ${presigned.name}: ${text || put.statusText}`);
  }
  return { ok: true, key: presigned.key };
}
