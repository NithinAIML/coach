// import type { NextApiRequest, NextApiResponse } from 'next';
// import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
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
//     console.warn(`[put] CA bundle not found at ${abs}`);
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
//   const kind = (body.kind || '').toString().trim();

//   // normalize any of: contactEmail | teamEmail | email
//   const email: string =
//     (body.contactEmail || body.teamEmail || body.email || '').toString().trim();

//   if (!kind)  return res.status(400).json({ ok: false, error: 'kind is required' });
//   if (!email) return res.status(400).json({ ok: false, error: 'teamEmail/contactEmail is required' });

//   body.contactEmail = email;
//   body.teamEmail = email;
//   body.email = email;

//   const key = `coach/${sanitizeEmail(email)}/${kind}.json`;

//   try {
//     const agent = buildHttpsAgent();
//     const s3 = new S3Client({
//       region,
//       ...(agent ? { requestHandler: new NodeHttpHandler({ httpsAgent: agent }) } : {}),
//     });

//     await s3.send(
//       new PutObjectCommand({
//         Bucket: bucket,
//         Key: key,
//         Body: JSON.stringify(body, null, 2),
//         ContentType: 'application/json',
//       })
//     );

//     return res.status(200).json({
//       ok: true,
//       key,
//       message:
//         kind === 'registration'
//           ? 'Team details saved successfully.'
//           : 'Saved successfully.',
//     });
//   } catch (err: any) {
//     console.error('S3 put error:', err);
//     return res.status(500).json({ ok: false, error: err?.message || 'S3 put failed' });
//   }
// }


// pages/api/put.ts
import type { NextApiRequest, NextApiResponse } from 'next';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';

const REGION = process.env.AWS_REGION || 'us-east-1';
const BUCKET = process.env.COACH_BUCKET as string;
const PREFIX = (process.env.COACH_PREFIX || 'coach/').replace(/^\/?/, '').replace(/\/?$/, '/') // ensure "coach/"

const s3 = new S3Client({ region: REGION });

function safeId(input: string): string {
  return (input || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}

function buildKey(payload: any): string {
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const email = safeId(payload.teamEmail || payload.contactEmail || 'unknown');
  if (payload.kind === 'registration') {
    return `${PREFIX}registrations/${email}/registration.json`;
  }
  if (payload.kind === 'sources') {
    return `${PREFIX}knowledge/${email}/sources-${ts}.json`;
  }
  return `${PREFIX}misc/${email}/data-${ts}.json`;
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (!BUCKET) return res.status(500).send('Missing COACH_BUCKET');
  if (req.method !== 'POST') return res.status(405).end('Method not allowed');

  try {
    const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body || {};
    const { kind, teamEmail, contactEmail } = body || {};
    if (!kind) return res.status(400).send('Field "kind" is required');
    if (!teamEmail && !contactEmail) return res.status(400).send('Fields "teamEmail" or "contactEmail" are required');

    const key = buildKey(body);
    const stored = {
      ...body,
      serverSavedAt: new Date().toISOString(),
    };

    await s3.send(
      new PutObjectCommand({
        Bucket: BUCKET,
        Key: key,
        Body: JSON.stringify(stored, null, 2),
        ContentType: 'application/json',
      })
    );

    return res.status(200).json({ ok: true, key });
  } catch (err: any) {
    console.error('PUT /api/put error:', err);
    return res.status(500).send(err?.message || 'Failed to write to S3');
  }
}
