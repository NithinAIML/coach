// pages/api/registration-status.ts
import type { NextApiRequest, NextApiResponse } from 'next';
import { S3Client, HeadObjectCommand } from '@aws-sdk/client-s3';

const REGION = process.env.AWS_REGION || 'us-east-1';
const BUCKET = process.env.COACH_BUCKET as string;
const PREFIX = (process.env.COACH_PREFIX || 'coach/').replace(/^\/?/, '').replace(/\/?$/, '/');

const s3 = new S3Client({ region: REGION });

function safeId(input: string): string {
  return (input || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (!BUCKET) return res.status(500).json({ registered: false, error: 'Missing COACH_BUCKET' });
  if (req.method !== 'GET') return res.status(405).end('Method not allowed');

  try {
    const email = (req.query.email as string) || '';
    if (!email) return res.status(400).json({ registered: false, error: 'email is required' });

    const key = `${PREFIX}registrations/${safeId(email)}/registration.json`;
    try {
      await s3.send(new HeadObjectCommand({ Bucket: BUCKET, Key: key }));
      return res.status(200).json({ registered: true, key });
    } catch (_e: any) {
      // Not found or no perms
      return res.status(200).json({ registered: false });
    }
  } catch (err: any) {
    console.error('GET /api/registration-status error:', err);
    return res.status(200).json({ registered: false });
  }
}
