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

// src/pages/api/put.ts
import type { NextApiRequest, NextApiResponse } from "next";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const REGION = process.env.AWS_REGION || "us-east-1";
const BUCKET = process.env.COACH_BUCKET as string;
const PREFIX = (process.env.COACH_PREFIX || "coach/").replace(/^\/+|\/+$/g, "") + "/";

const s3 = new S3Client({
  region: REGION,
  // Credentials will be picked up from env automatically if set.
  // Leave empty for default provider chain.
});

type Body = {
  kind: "registration" | "sources";
  teamEmail: string;
  [k: string]: any;
};

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });
  if (!BUCKET) return res.status(500).json({ error: "COACH_BUCKET not configured" });

  let body: Body;
  try {
    body = req.body && typeof req.body === "string" ? JSON.parse(req.body) : req.body;
  } catch {
    return res.status(400).json({ error: "Invalid JSON" });
  }

  if (!body?.kind) return res.status(400).json({ error: "kind is required" });
  if (!body?.teamEmail || typeof body.teamEmail !== "string") {
    return res.status(400).json({ error: "teamEmail is required" });
  }

  // Normalize team key (email) into path-safe token
  const teamKey = encodeURIComponent(body.teamEmail.toLowerCase().trim());

  const now = new Date();
  const ts = now.toISOString().replace(/[:.]/g, "-"); // 2025-08-28T09-12-33-123Z
  let key: string;

  if (body.kind === "registration") {
    key = `${PREFIX}${teamKey}/registration.json`;
  } else if (body.kind === "sources") {
    key = `${PREFIX}${teamKey}/knowledge/sources-${ts}.json`;
  } else {
    return res.status(400).json({ error: "Unknown kind" });
  }

  try {
    const put = new PutObjectCommand({
      Bucket: BUCKET,
      Key: key,
      Body: Buffer.from(JSON.stringify(body, null, 2), "utf-8"),
      ContentType: "application/json",
      // ServerSideEncryption: "AES256", // uncomment if you want SSE
    });
    await s3.send(put);
    return res.status(200).json({ ok: true, key });
  } catch (e: any) {
    console.error("S3 put error:", e);
    return res.status(500).json({ error: e?.message || "Failed to save to S3" });
  }
}
