// // src/pages/api/upload.ts
// import type { NextApiRequest, NextApiResponse } from "next";
// import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
// import formidable, { File as FormidableFile } from "formidable";
// import fs from "fs";

// export const config = {
//   api: { bodyParser: false }, // important: let formidable handle the stream
// };

// const REGION = process.env.AWS_REGION || "us-east-1";
// const BUCKET = process.env.COACH_BUCKET!;
// const PREFIX = (process.env.COACH_PREFIX || "coach/teams").replace(/^\/+|\/+$/g, "");
// const s3 = new S3Client({ region: REGION });

// function keyJoin(...parts: string[]) {
//   return parts.filter(Boolean).map(p => p.replace(/^\/+|\/+$/g, "")).join("/");
// }
// function ts() {
//   const d = new Date();
//   return d.toISOString().replace(/[-:.]/g, "").slice(0, 15) + "Z";
// }
// function asString(v: string | string[] | undefined): string {
//   if (!v) return "";
//   return Array.isArray(v) ? String(v[0] ?? "") : String(v);
// }

// function parseForm(req: NextApiRequest) {
//   const form = formidable({ multiples: true });
//   return new Promise<{ fields: formidable.Fields; files: formidable.Files }>((resolve, reject) => {
//     form.parse(req, (err, fields, files) => (err ? reject(err) : resolve({ fields, files })));
//   });
// }

// export default async function handler(req: NextApiRequest, res: NextApiResponse) {
//   if (req.method !== "POST") return res.status(405).json({ error: "Method Not Allowed" });
//   try {
//     if (!BUCKET) return res.status(500).json({ error: "Missing COACH_BUCKET env" });

//     const { fields, files } = await parseForm(req);
//     const teamEmail = asString(fields.teamEmail)?.trim();
//     if (!teamEmail) return res.status(400).json({ error: "teamEmail is required" });

//     const stamp = ts();
//     const uploaded: { key: string; name: string; size: number }[] = [];

//     const anyFiles = (files.files ??
//       files.file ??
//       files.upload ??
//       []) as unknown as FormidableFile | FormidableFile[];

//     const fileArr = Array.isArray(anyFiles) ? anyFiles : anyFiles ? [anyFiles] : [];

//     for (const f of fileArr) {
//       const name = f.originalFilename || f.newFilename || "upload.bin";
//       const key = keyJoin(PREFIX, teamEmail, "uploads", stamp, name);
//       const readStream = fs.createReadStream(f.filepath);
//       await s3.send(
//         new PutObjectCommand({
//           Bucket: BUCKET,
//           Key: key,
//           Body: readStream,
//           ContentType: f.mimetype || "application/octet-stream",
//         })
//       );
//       uploaded.push({ key, name, size: f.size || 0 });
//     }

//     return res.status(200).json({ ok: true, uploaded });
//   } catch (err: any) {
//     console.error("/api/upload error:", err);
//     return res.status(500).json({ ok: false, error: err?.message || "Unknown error" });
//   }
// }

import type { NextApiRequest, NextApiResponse } from 'next';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

import https from 'https';
import fs from 'fs';
import { NodeHttpHandler } from '@smithy/node-http-handler';

// ---- env ----
const REGION = process.env.AWS_REGION!;
const BUCKET = process.env.COACH_BUCKET!;
const PREFIX = process.env.COACH_PREFIX || 'coach/';

// Build an HTTPS agent that trusts your enterprise CA bundle if provided.
function buildHttpsAgent() {
  const caPath = process.env.AWS_CA_BUNDLE || process.env.NODE_EXTRA_CA_CERTS;
  if (caPath && fs.existsSync(caPath)) {
    return new https.Agent({
      keepAlive: true,
      ca: fs.readFileSync(caPath),
    });
  }
  return new https.Agent({ keepAlive: true });
}

const s3 = new S3Client({
  region: REGION,
  requestHandler: new NodeHttpHandler({ httpsAgent: buildHttpsAgent() }),
});

type IncomingFile = { name: string; type?: string };

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });
  if (!REGION) return res.status(500).json({ error: 'Missing AWS_REGION env' });
  if (!BUCKET) return res.status(500).json({ error: 'Missing COACH_BUCKET env' });

  try {
    const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body || {};
    const contactEmail: string | undefined = body?.contactEmail || body?.teamEmail; // support either
    const files: IncomingFile[] = Array.isArray(body?.files) ? body.files : [];

    if (!contactEmail) return res.status(400).json({ error: 'contactEmail is required' });
    if (files.length === 0) return res.status(400).json({ error: 'files[] required' });

    const safeEmail = encodeURIComponent(contactEmail);

    const presigned = await Promise.all(
      files.map(async (f) => {
        if (!f?.name) throw new Error('file name missing');
        const key = `${PREFIX}uploads/${safeEmail}/${encodeURIComponent(f.name)}`;
        const cmd = new PutObjectCommand({
          Bucket: BUCKET,
          Key: key,
          ContentType: f.type || 'application/octet-stream',
        });
        const url = await getSignedUrl(s3, cmd, { expiresIn: 60 * 10 }); // 10 minutes
        return { name: f.name, key, url };
      })
    );

    return res.status(200).json({ ok: true, presigned });
  } catch (err: any) {
    console.error('Presign error:', err);
    return res.status(500).json({ error: err?.message || 'presign failed' });
  }
}
