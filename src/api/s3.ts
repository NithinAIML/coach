// src/pages/api/s3.ts
import type { NextApiRequest, NextApiResponse } from "next";
import { S3Client, HeadObjectCommand } from "@aws-sdk/client-s3";

const REGION = process.env.AWS_REGION || "us-east-1";
const BUCKET = process.env.COACH_BUCKET!;
const s3 = new S3Client({ region: REGION });

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "POST") return res.status(405).json({ error: "Method Not Allowed" });
  try {
    const { key } = typeof req.body === "string" ? JSON.parse(req.body) : req.body;
    if (!key) return res.status(400).json({ error: "key required" });
    if (!BUCKET) return res.status(500).json({ error: "Missing COACH_BUCKET env" });

    try {
      await s3.send(new HeadObjectCommand({ Bucket: BUCKET, Key: key }));
      return res.status(200).json({ ok: true, exists: true });
    } catch {
      return res.status(200).json({ ok: true, exists: false });
    }
  } catch (err: any) {
    console.error("/api/s3 error:", err);
    return res.status(500).json({ ok: false, error: err?.message || "Unknown error" });
  }
}
