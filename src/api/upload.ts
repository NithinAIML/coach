// import type { NextApiRequest, NextApiResponse } from 'next';
// import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
// import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
// import { NodeHttpHandler } from '@aws-sdk/node-http-handler';
// import https from 'https';
// import fs from 'fs';
// import path from 'path';

// function sanitizeEmail(e: string) {
//   return e.replace(/@/g, '_at_').replace(/[^a-zA-Z0-9._-]/g, '_');
// }

// function buildHttpsAgent(): https.Agent | undefined {
//   const p =
//     process.env.CUSTOM_CA_PEM_PATH ||
//     process.env.AWS_CA_BUNDLE ||
//     process.env.NODE_EXTRA_CA_CERTS;

//   if (!p) return undefined;

//   const abs = path.isAbsolute(p) ? p : path.join(process.cwd(), p);
//   if (!fs.existsSync(abs)) {
//     console.warn(`[upload] CA bundle not found at ${abs}`);
//     return undefined;
//   }
//   const ca = fs.readFileSync(abs);
//   return new https.Agent({ keepAlive: true, ca });
// }

// export default async function handler(req: NextApiRequest, res: NextApiResponse) {
//   if (req.method !== 'POST') return res.status(405).json({ ok: false, error: 'Method Not Allowed' });

//   const region = process.env.AWS_REGION || 'us-east-1';
//   const bucket = process.env.COACH_BUCKET;
//   if (!bucket) return res.status(500).json({ ok: false, error: 'Missing COACH_BUCKET env' });

//   const body = typeof req.body === 'string' ? JSON.parse(req.body || '{}') : (req.body || {});
//   const email: string =
//     (body.email || body.contactEmail || body.teamEmail || '').toString().trim();
//   const files: Array<{ name: string; type?: string }> = Array.isArray(body.files) ? body.files : [];

//   if (!email) return res.status(400).json({ ok: false, error: 'teamEmail/contactEmail is required' });
//   if (!files.length) return res.status(400).json({ ok: false, error: 'files array is required' });

//   try {
//     const agent = buildHttpsAgent();
//     const s3 = new S3Client({
//       region,
//       ...(agent ? { requestHandler: new NodeHttpHandler({ httpsAgent: agent }) } : {}),
//     });

//     const owner = sanitizeEmail(email);
//     const uploads: Array<{ name: string; key: string; url: string }> = [];

//     for (const f of files) {
//       const key = `coach/${owner}/uploads/${Date.now()}-${encodeURIComponent(f.name)}`;
//       const cmd = new PutObjectCommand({
//         Bucket: bucket,
//         Key: key,
//         ContentType: f.type || 'application/octet-stream',
//       });
//       const url = await getSignedUrl(s3, cmd, { expiresIn: 900 }); // 15 minutes
//       uploads.push({ name: f.name, key, url });
//     }

//     res.status(200).json({ ok: true, uploads });
//   } catch (err: any) {
//     console.error('presign error:', err);
//     res.status(500).json({ ok: false, error: err?.message || 'presign failed' });
//   }
// }


// pages/api/upload.ts
// import type { NextApiRequest, NextApiResponse } from 'next';
// import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
// import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

// const REGION = process.env.AWS_REGION || 'us-east-1';
// const BUCKET = process.env.COACH_BUCKET as string;
// const PREFIX = (process.env.COACH_PREFIX || 'coach/').replace(/^\/?/, '').replace(/\/?$/, '/');

// const s3 = new S3Client({ region: REGION });

// function safeId(input: string): string {
//   return (input || '')
//     .trim()
//     .toLowerCase()
//     .replace(/[^a-z0-9._-]+/g, '-')
//     .replace(/-+/g, '-')
//     .replace(/^-|-$/g, '');
// }
// function rand(n = 8) {
//   return Math.random().toString(16).slice(2, 2 + n);
// }
// function safeFileName(name: string): string {
//   // keep extension if present
//   const trimmed = (name || '').replace(/\s+/g, '_');
//   return trimmed.replace(/[^a-zA-Z0-9._-]/g, '-');
// }

// export default async function handler(req: NextApiRequest, res: NextApiResponse) {
//   if (!BUCKET) return res.status(500).send('Missing COACH_BUCKET');
//   if (req.method !== 'POST') return res.status(405).end('Method not allowed');

//   try {
//     const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body || {};
//     const email = body?.email as string;
//     const files = (body?.files || []) as Array<{ name: string; type?: string; size?: number }>;

//     if (!email) return res.status(400).send('Field "email" is required');
//     if (!Array.isArray(files) || files.length === 0) return res.status(400).send('Field "files" must be a non-empty array');

//     const userId = safeId(email);
//     const day = new Date();
//     const dayPath = `${day.getUTCFullYear()}/${(day.getUTCMonth() + 1).toString().padStart(2, '0')}/${day.getUTCDate().toString().padStart(2, '0')}`;

//     const urls = await Promise.all(
//       files.map(async (f) => {
//         const baseName = safeFileName(f.name || `file-${rand(6)}`);
//         const key = `${PREFIX}uploads/${userId}/${dayPath}/${Date.now()}-${rand(6)}-${baseName}`;

//         const cmd = new PutObjectCommand({
//           Bucket: BUCKET,
//           Key: key,
//           ContentType: f.type || 'application/octet-stream',
//         });

//         // Default expiry 15 minutes
//         const url = await getSignedUrl(s3, cmd, { expiresIn: 15 * 60 });

//         return { name: f.name, key, url };
//       })
//     );

//     return res.status(200).json({ urls });
//   } catch (err: any) {
//     console.error('POST /api/upload error:', err);
//     return res.status(500).send(err?.message || 'Failed to create presigned URLs');
//   }
// }


import type { NextApiRequest, NextApiResponse } from 'next';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

const REGION = process.env.AWS_REGION || 'us-east-1';
const BUCKET = process.env.COACH_BUCKET as string;
const PREFIX = (process.env.COACH_PREFIX || 'coach/teams').replace(/^\/+|\/+$/g, '');
const s3 = new S3Client({ region: REGION });

function safeId(input: string): string {
  return (input || '').trim().toLowerCase().replace(/[^a-z0-9._-]+/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '');
}
function rand(n = 8) { return Math.random().toString(16).slice(2, 2 + n); }
function safeFileName(name: string): string {
  const trimmed = (name || '').replace(/\s+/g, '_');
  return trimmed.replace(/[^a-zA-Z0-9._-]/g, '-');
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (!BUCKET) return res.status(500).send('Missing COACH_BUCKET');
  if (req.method !== 'POST') return res.status(405).end('Method not allowed');

  try {
    const body = typeof req.body === 'string' ? JSON.parse(req.body) : (req.body || {});
    const email = body?.email as string;
    const files = (body?.files || []) as Array<{ name: string; type?: string; size?: number }>;

    if (!email) return res.status(400).send('Field "email" is required');
    if (!Array.isArray(files) || files.length === 0) return res.status(400).send('Field "files" must be a non-empty array');

    const userId = safeId(email);
    const day = new Date();
    const dayPath = `${day.getUTCFullYear()}/${String(day.getUTCMonth() + 1).padStart(2, '0')}/${String(day.getUTCDate()).padStart(2, '0')}`;

    const uploads = await Promise.all(files.map(async (f) => {
      const baseName = safeFileName(f.name || `file-${rand(6)}`);
      const key = `${PREFIX}/uploads/${userId}/${dayPath}/${Date.now()}-${rand(6)}-${baseName}`;
      const cmd = new PutObjectCommand({ Bucket: BUCKET, Key: key, ContentType: f.type || 'application/octet-stream' });
      const url = await getSignedUrl(s3, cmd, { expiresIn: 15 * 60 }); // 15 min
      return { name: f.name, key, url };
    }));

    // IMPORTANT: return "uploads" (matches src/utils/s3.ts)
    return res.status(200).json({ uploads });
  } catch (err: any) {
    console.error('POST /api/upload error:', err);
    return res.status(500).send(err?.message || 'Failed to create presigned URLs');
  }
}
