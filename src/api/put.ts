// import type { NextApiRequest, NextApiResponse } from 'next';
// import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';

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
//       `[put.ts] Could not read CA bundle at ${abs}. Using default trust store. Error:`,
//       (e as any)?.message || e
//     );
//     return new https.Agent({ keepAlive: true });
//   }
// }

// const s3 = new S3Client({
//   region: REGION,
//   requestHandler: new NodeHttpHandler({ httpsAgent: buildHttpsAgent() }),
// });

// export default async function handler(req: NextApiRequest, res: NextApiResponse) {
//   if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });
//   if (!REGION) return res.status(500).json({ error: 'Missing AWS_REGION env' });
//   if (!BUCKET) return res.status(500).json({ error: 'Missing COACH_BUCKET env' });

//   try {
//     const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body || {};
//     const kind = body?.kind;
//     const contactEmail: string | undefined = body?.contactEmail || body?.teamEmail;

//     if (!kind) return res.status(400).json({ error: 'kind is required' });
//     if (!contactEmail) return res.status(400).json({ error: 'contactEmail is required' });

//     const safeEmail = encodeURIComponent(contactEmail);
//     const key = `${PREFIX}${kind}/${safeEmail}/${Date.now()}.json`;

//     await s3.send(
//       new PutObjectCommand({
//         Bucket: BUCKET,
//         Key: key,
//         Body: JSON.stringify(body),
//         ContentType: 'application/json',
//       })
//     );

//     return res.status(200).json({ ok: true, key });
//   } catch (err: any) {
//     console.error('S3 put error:', err);
//     return res.status(500).json({ error: err?.message || 'S3 put failed' });
//   }
// }

import type { NextApiRequest, NextApiResponse } from 'next';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
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

  // Body can be string or already parsed
  const body = typeof req.body === 'string' ? JSON.parse(req.body || '{}') : (req.body || {});
  const kind = (body.kind || '').toString().trim();

  // Accept either `contactEmail` or `teamEmail`
  const email = ((body.contactEmail || body.teamEmail || '') as string).trim();
  if (!kind)  return res.status(400).json({ ok: false, error: 'kind is required' });
  if (!email) return res.status(400).json({ ok: false, error: 'teamEmail/contactEmail is required' });

  const emailSafe = sanitizeEmail(email);
  const key = `coach/${emailSafe}/${kind}.json`;

  try {
    const s3 = new S3Client({ region, requestHandler: { ...new (require('@aws-sdk/node-http-handler').NodeHttpHandler)({ httpsAgent: getHttpsAgent() }) } as any });

    const put = new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: JSON.stringify(body, null, 2),
      ContentType: 'application/json',
    });

    await s3.send(put);
    return res.status(200).json({ ok: true, key, message: 'Saved to S3' });
  } catch (err: any) {
    console.error('S3 put error:', err);
    const msg = err?.message || 'S3 put failed';
    return res.status(500).json({ ok: false, error: msg });
  }
}
