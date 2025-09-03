// import type { NextApiRequest, NextApiResponse } from 'next';
// import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
// import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

// import https from 'https';
// import fs from 'fs';
// import path from 'path';
// import { NodeHttpHandler } from '@smithy/node-http-handler';

// const REGION = process.env.AWS_REGION!;
// const BUCKET = process.env.COACH_BUCKET!;
// const PREFIX = process.env.COACH_PREFIX || 'coach/';

// function buildHttpsAgent() {
//   const raw = process.env.AWS_CA_BUNDLE || process.env.NODE_EXTRA_CA_CERTS;
//   if (!raw) return new https.Agent({ keepAlive: true });

//   const abs = path.isAbsolute(raw) ? raw : path.resolve(process.cwd(), raw);
//   try {
//     const ca = fs.readFileSync(abs);
//     return new https.Agent({ keepAlive: true, ca });
//   } catch (e) {
//     console.warn(
//       `[upload.ts] Could not read CA bundle at ${abs}. Using default trust store. Error:`,
//       (e as any)?.message || e
//     );
//     return new https.Agent({ keepAlive: true });
//   }
// }

// const s3 = new S3Client({
//   region: REGION,
//   requestHandler: new NodeHttpHandler({ httpsAgent: buildHttpsAgent() }),
// });

// type IncomingFile = { name: string; type?: string };

// export default async function handler(req: NextApiRequest, res: NextApiResponse) {
//   if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });
//   if (!REGION) return res.status(500).json({ error: 'Missing AWS_REGION env' });
//   if (!BUCKET) return res.status(500).json({ error: 'Missing COACH_BUCKET env' });

//   try {
//     const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body || {};
//     const contactEmail: string | undefined = body?.contactEmail || body?.teamEmail;
//     const files: IncomingFile[] = Array.isArray(body?.files) ? body.files : [];

//     if (!contactEmail) return res.status(400).json({ error: 'contactEmail is required' });
//     if (files.length === 0) return res.status(400).json({ error: 'files[] required' });

//     const safeEmail = encodeURIComponent(contactEmail);

//     const presigned = await Promise.all(
//       files.map(async (f) => {
//         if (!f?.name) throw new Error('file name missing');
//         const key = `${PREFIX}uploads/${safeEmail}/${encodeURIComponent(f.name)}`;
//         const cmd = new PutObjectCommand({
//           Bucket: BUCKET,
//           Key: key,
//           ContentType: f.type || 'application/octet-stream',
//         });
//         const url = await getSignedUrl(s3, cmd, { expiresIn: 60 * 10 }); // 10 minutes
//         return { name: f.name, key, url };
//       })
//     );

//     return res.status(200).json({ ok: true, presigned });
//   } catch (err: any) {
//     console.error('Presign error:', err);
//     return res.status(500).json({ error: err?.message || 'presign failed' });
//   }
// }

import type { NextApiRequest, NextApiResponse } from 'next';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import https from 'https';
import fs from 'fs';

function sanitizeEmail(e: string) {
  return e.replace(/@/g, '_at_').replace(/[^a-zA-Z0-9._-]/g, '_');
}

function getHttpsAgent() {
  const caPath = process.env.CUSTOM_CA_PEM_PATH || process.env.AWS_CA_BUNDLE;
  if (caPath && fs.existsSync(caPath)) {
    return new https.Agent({ keepAlive: true, ca: fs.readFileSync(caPath) });
  }
  return new https.Agent({ keepAlive: true });
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') return res.status(405).json({ ok: false, error: 'Method Not Allowed' });

  const bucket = process.env.COACH_BUCKET;
  const region = process.env.AWS_REGION || 'us-east-1';
  if (!bucket) return res.status(500).json({ ok: false, error: 'Missing COACH_BUCKET env' });

  const body = typeof req.body === 'string' ? JSON.parse(req.body || '{}') : (req.body || {});
  const email = ((body.email || body.contactEmail || body.teamEmail || '') as string).trim();
  const files: Array<{ name: string; type?: string }> = Array.isArray(body.files) ? body.files : [];

  if (!email) return res.status(400).json({ ok: false, error: 'email/contactEmail/teamEmail is required' });
  if (!files.length) return res.status(400).json({ ok: false, error: 'files array is required' });

  try {
    const s3 = new S3Client({ region, requestHandler: { ...new (require('@aws-sdk/node-http-handler').NodeHttpHandler)({ httpsAgent: getHttpsAgent() }) } as any });
    const emailSafe = sanitizeEmail(email);

    const out: Array<{ name: string; key: string; url: string }> = [];
    for (const f of files) {
      const key = `coach/${emailSafe}/uploads/${Date.now()}-${encodeURIComponent(f.name)}`;
      const cmd = new PutObjectCommand({ Bucket: bucket, Key: key, ContentType: f.type || 'application/octet-stream' });
      const url = await getSignedUrl(s3, cmd, { expiresIn: 900 }); // 15 min
      out.push({ name: f.name, key, url });
    }

    return res.status(200).json({ ok: true, uploads: out });
  } catch (err: any) {
    console.error('presign error:', err);
    return res.status(500).json({ ok: false, error: err?.message || 'presign failed' });
  }
}
