// src/pages/api/process.ts
import type { NextApiRequest, NextApiResponse } from "next";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const REGION = process.env.AWS_REGION || "us-east-1";
const BUCKET = process.env.COACH_BUCKET!;
const PREFIX = (process.env.COACH_PREFIX || "coach/teams").replace(/^\/+|\/+$/g, "");
const s3 = new S3Client({ region: REGION });

function keyJoin(...parts: string[]) {
  return parts.filter(Boolean).map(p => p.replace(/^\/+|\/+$/g, "")).join("/");
}
function ts() {
  const d = new Date();
  return d.toISOString().replace(/[-:.]/g, "").slice(0, 15) + "Z";
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "POST") return res.status(405).json({ error: "Method Not Allowed" });
  try {
    if (!BUCKET) return res.status(500).json({ error: "Missing COACH_BUCKET env" });
    const { teamEmail, sourcesKey } = typeof req.body === "string" ? JSON.parse(req.body) : req.body;
    if (!teamEmail) return res.status(400).json({ error: "teamEmail required" });

    const key = keyJoin(PREFIX, teamEmail, "processing", `trigger-${ts()}.json`);
    const payload = { teamEmail, sourcesKey: sourcesKey || null, at: new Date().toISOString() };

    await s3.send(
      new PutObjectCommand({
        Bucket: BUCKET,
        Key: key,
        Body: Buffer.from(JSON.stringify(payload, null, 2)),
        ContentType: "application/json",
      })
    );

    return res.status(200).json({ ok: true, key });
  } catch (err: any) {
    console.error("/api/process error:", err);
    return res.status(500).json({ ok: false, error: err?.message || "Unknown error" });
  }
}
