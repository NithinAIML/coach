import type { NextApiRequest, NextApiResponse } from 'next';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import fs from 'fs';
import https from 'https';

function sanitizeEmail(e: string) {
  return e.replace(/@/g, '_at_').replace(/[^a-zA-Z0-9._-]/g, '_');
}

function buildOptionalRequestHandler() {
  try {
    // Use if installed; otherwise fall through.
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
    // Module not present; ok to ignore (default handler will be used).
  }
  return undefined;
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') return res.status(405).json({ ok: false, error: 'Method Not Allowed' });

  const bucket = process.env.COACH_BUCKET;
  const region = process.env.AWS_REGION || 'us-east-1';
  if (!bucket) return res.status(500).json({ ok: false, error: 'Missing COACH_BUCKET env' });

  const body = typeof req.body === 'string' ? JSON.parse(req.body || '{}') : (req.body || {});
  const kind = (body.kind || '').toString().trim();

  const email: string =
    (body.contactEmail || body.teamEmail || body.email || '').toString().trim();

  if (!kind)  return res.status(400).json({ ok: false, error: 'kind is required' });
  if (!email) return res.status(400).json({ ok: false, error: 'Email (contactEmail/teamEmail) is required' });

  // normalize both keys for compatibility
  body.contactEmail = email;
  body.teamEmail = email;
  body.email = email;

  const key = `coach/${sanitizeEmail(email)}/${kind}.json`;

  try {
    const requestHandler = buildOptionalRequestHandler();
    const s3 = new S3Client({ region, ...(requestHandler ? { requestHandler } : {}) });

    await s3.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: JSON.stringify(body, null, 2),
        ContentType: 'application/json',
      })
    );

    return res.status(200).json({
      ok: true,
      key,
      message: kind === 'registration'
        ? 'Team details saved successfully.'
        : 'Saved successfully.',
    });
  } catch (err: any) {
    console.error('S3 put error:', err);
    return res.status(500).json({ ok: false, error: err?.message || 'S3 put failed' });
  }
}
