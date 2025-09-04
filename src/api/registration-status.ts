import type { NextApiRequest, NextApiResponse } from 'next';
import https from 'https';
import fs from 'fs';
import { S3Client, HeadObjectCommand } from '@aws-sdk/client-s3';
import { NodeHttpHandler } from '@smithy/node-http-handler';

function readPemFromEnv(): string[] {
  const vars = ['AWS_CA_BUNDLE', 'NODE_EXTRA_CA_CERTS'] as const;
  const cas: string[] = [];
  for (const v of vars) {
    const val = process.env[v];
    if (!val) continue;
    if (val.includes('-----BEGIN CERTIFICATE-----')) {
      // inline PEM (supports escaped \n)
      cas.push(val.replace(/\\n/g, '\n'));
    } else if (fs.existsSync(val)) {
      cas.push(fs.readFileSync(val, 'utf8'));
    }
  }
  return cas;
}

function getHttpsAgent(): https.Agent | undefined {
  const cas = readPemFromEnv();
  return cas.length ? new https.Agent({ keepAlive: true, ca: cas }) : undefined;
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

  const region = process.env.AWS_REGION || 'us-east-1';
  const agent = getHttpsAgent();
  const s3 = new S3Client({
    region,
    requestHandler: agent ? new NodeHttpHandler({ httpsAgent: agent }) : undefined,
  });

  try {
    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: regKey(email) }));
    return res.status(200).json({ registered: true });
  } catch (err: any) {
    if (err?.$metadata?.httpStatusCode === 404) {
      return res.status(200).json({ registered: false });
    }
    console.error('registration-status error:', err);
    return res.status(500).json({ error: 'status check failed' });
  }
}
