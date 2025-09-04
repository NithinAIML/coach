import type { NextApiRequest, NextApiResponse } from 'next';
import { S3Client, HeadObjectCommand } from '@aws-sdk/client-s3';
import { NodeHttpHandler } from '@aws-sdk/node-http-handler';
import fs from 'fs';
import https from 'https';

function getHttpsAgent(): https.Agent | undefined {
  const pem = process.env.COACH_CA_CERT_PEM;
  const path = process.env.COACH_CA_CERT_PATH;
  let ca: string | undefined;

  if (pem && pem.includes('-----BEGIN CERTIFICATE-----')) {
    ca = pem.replace(/\\n/g, '\n');
  } else if (path) {
    try { ca = fs.readFileSync(path, 'utf8'); } catch { /* ignore */ }
  }
  return ca ? new https.Agent({ keepAlive: true, ca }) : undefined;
}

function getS3() {
  const region = process.env.AWS_REGION || 'us-east-1';
  const httpsAgent = getHttpsAgent();
  return new S3Client({
    region,
    requestHandler: httpsAgent ? new NodeHttpHandler({ httpsAgent }) : undefined,
  });
}

function regKey(email: string) {
  const prefix = process.env.COACH_PREFIX || 'coach';
  return `${prefix}/${encodeURIComponent(email)}/registration.json`;
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    res.setHeader('Allow', ['GET']);
    return res.status(405).json({ error: 'Method Not Allowed' });
  }

  const bucket = process.env.COACH_BUCKET;
  if (!bucket) return res.status(500).json({ error: 'Missing COACH_BUCKET env' });

  const email = (req.query.email as string || '').trim();
  if (!email) return res.status(400).json({ error: 'email is required' });

  const s3 = getS3();

  try {
    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: regKey(email) }));
    return res.status(200).json({ registered: true });
  } catch (err: any) {
    // Not found -> not registered. Everything else -> 500.
    const code = err?.$metadata?.httpStatusCode;
    if (code === 404) return res.status(200).json({ registered: false });

    console.error('registration-status error:', err);
    return res.status(500).json({ error: 'status check failed' });
  }
}
