import type { NextApiRequest, NextApiResponse } from 'next';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import fs from 'fs';
import https from 'https';

function sanitizeEmail(e: string) {
  return e.replace(/@/g, '_at_').replace(/[^a-zA-Z0-9._-]/g, '_');
}

function buildOptionalRequestHandler() {
  try {
    const { NodeHttpHandler } = require('@aws-sdk/node-http-handler');
    const caPath =
      process.env.CUSTOM_CA_PEM_PATH ||
      process.env.AWS_CA_BUNDLE ||
      process.env.NODE_EXTRA_CA_CERTS;

    if (caPath && fs.existsSync(caPath)) {
      const agent = new https.Agent({ keepAlive: true, ca: fs.readFileSync(caPath) });
      return new NodeHttpHandler({ httpsAgent: agent });
    }
  } catch {
    // ok – use default handler
  }
  return undefined;
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') return res.status(405).json({ ok: false, error: 'Method Not Allowed' });

  const bucket = process.env.COACH_BUCKET;
  const region = process.env.AWS_REGION || 'us-east-1';
  if (!bucket) return res.status(500).json({ ok: false, error: 'Missing COACH_BUCKET env' });

  const body = typeof req.body === 'string' ? JSON.parse(req.body || '{}') : (req.body || {});
  const email: string =
    (body.email || body.contactEmail || body.teamEmail || '').toString().trim();
  const files: Array<{ name: string; type?: string }> = Array.isArray(body.files) ? body.files : [];

  if (!email) return res.status(400).json({ ok: false, error: 'Email (contactEmail/teamEmail) is required' });
  if (!files.length) return res.status(400).json({ ok: false, error: 'files array is required' });

  try {
    const requestHandler = buildOptionalRequestHandler();
    const s3 = new S3Client({ region, ...(requestHandler ? { requestHandler } : {}) });

    const owner = sanitizeEmail(email);
    const uploads: Array<{ name: string; key: string; url: string }> = [];

    for (const f of files) {
      const key = `coach/${owner}/uploads/${Date.now()}-${encodeURIComponent(f.name)}`;
      const cmd = new PutObjectCommand({ Bucket: bucket, Key: key, ContentType: f.type || 'application/octet-stream' });
      const url = await getSignedUrl(s3, cmd, { expiresIn: 900 });
      uploads.push({ name: f.name, key, url });
    }

    res.status(200).json({ ok: true, uploads });
  } catch (err: any) {
    console.error('presign error:', err);
    res.status(500).json({ ok: false, error: err?.message || 'presign failed' });
  }
}
