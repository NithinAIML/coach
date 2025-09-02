// // src/pages/api/put.ts
// import type { NextApiRequest, NextApiResponse } from "next";
// import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

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

// export default async function handler(req: NextApiRequest, res: NextApiResponse) {
//   if (req.method !== "POST") return res.status(405).json({ error: "Method Not Allowed" });
//   try {
//     if (!BUCKET) return res.status(500).json({ error: "Missing COACH_BUCKET env" });
//     const body = typeof req.body === "string" ? JSON.parse(req.body) : req.body;

//     const kind = String(body?.kind || "").toLowerCase(); // "registration" | "sources" | ...
//     const teamEmail = String(body?.teamEmail || "").trim();
//     if (!kind || !teamEmail) return res.status(400).json({ error: "teamEmail and kind are required" });

//     const stamp = ts();
//     let key: string;
//     switch (kind) {
//       case "registration":
//         key = keyJoin(PREFIX, teamEmail, "registration", `${stamp}.json`);
//         break;
//       case "sources":
//         key = keyJoin(PREFIX, teamEmail, "sources", `${stamp}.json`);
//         break;
//       default:
//         key = keyJoin(PREFIX, teamEmail, "misc", `${kind}-${stamp}.json`);
//     }

//     // write timestamped
//     await s3.send(new PutObjectCommand({
//       Bucket: BUCKET,
//       Key: key,
//       Body: Buffer.from(JSON.stringify(body, null, 2)),
//       ContentType: "application/json",
//     }));

//     // update latest pointer
//     const latestKey =
//       kind === "registration"
//         ? keyJoin(PREFIX, teamEmail, "registration", "latest.json")
//         : kind === "sources"
//         ? keyJoin(PREFIX, teamEmail, "sources", "latest.json")
//         : keyJoin(PREFIX, teamEmail, "misc", `latest-${kind}.json`);

//     await s3.send(new PutObjectCommand({
//       Bucket: BUCKET,
//       Key: latestKey,
//       Body: Buffer.from(JSON.stringify({ ...body, _latestKeyOf: key }, null, 2)),
//       ContentType: "application/json",
//     }));

//     return res.status(200).json({ ok: true, key, latestKey });
//   } catch (err: any) {
//     console.error("/api/put error:", err);
//     return res.status(500).json({ ok: false, error: err?.message || "Unknown error" });
//   }
// }

import type { NextApiRequest, NextApiResponse } from 'next';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';

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

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });
  if (!REGION) return res.status(500).json({ error: 'Missing AWS_REGION env' });
  if (!BUCKET) return res.status(500).json({ error: 'Missing COACH_BUCKET env' });

  try {
    const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body || {};
    const kind = body?.kind;
    const contactEmail: string | undefined = body?.contactEmail || body?.teamEmail; // support either

    if (!kind) return res.status(400).json({ error: 'kind is required' });
    if (!contactEmail) return res.status(400).json({ error: 'contactEmail is required' });

    const safeEmail = encodeURIComponent(contactEmail);
    const key = `${PREFIX}${kind}/${safeEmail}/${Date.now()}.json`;

    await s3.send(
      new PutObjectCommand({
        Bucket: BUCKET,
        Key: key,
        Body: JSON.stringify(body),
        ContentType: 'application/json',
      })
    );

    return res.status(200).json({ ok: true, key });
  } catch (err: any) {
    console.error('S3 put error:', err);
    return res.status(500).json({ error: err?.message || 'S3 put failed' });
  }
}
