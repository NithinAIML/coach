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

// src/pages/api/upload.ts
import type { NextApiRequest, NextApiResponse } from "next";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

const REGION = process.env.AWS_REGION || "us-east-1";
const BUCKET = process.env.COACH_BUCKET as string;
const PREFIX = (process.env.COACH_PREFIX || "coach/").replace(/^\/+|\/+$/g, "") + "/";

const s3 = new S3Client({
  region: REGION,
  // Credentials auto from env (AWS_ACCESS_KEY_ID, etc)
});

type ReqFile = { name: string; type?: string; size?: number };
type ReqBody = { teamEmail: string; files: ReqFile[] };

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });
  if (!BUCKET) return res.status(500).json({ error: "COACH_BUCKET not configured" });

  let body: ReqBody;
  try {
    body = typeof req.body === "string" ? JSON.parse(req.body) : req.body;
  } catch {
    return res.status(400).json({ error: "Invalid JSON" });
  }

  if (!body?.teamEmail || typeof body.teamEmail !== "string") {
    return res.status(400).json({ error: "teamEmail is required" });
  }
  if (!Array.isArray(body.files) || body.files.length === 0) {
    return res.status(400).json({ error: "files is required" });
  }

  const teamKey = encodeURIComponent(body.teamEmail.toLowerCase().trim());
  const nowPrefix = new Date().toISOString().slice(0, 10); // YYYY-MM-DD

  try {
    const urls = await Promise.all(
      body.files.map(async (f) => {
        const cleanName = f.name.replace(/[^\w.\-]+/g, "_");
        const key = `${PREFIX}${teamKey}/uploads/${nowPrefix}/${Date.now()}-${cleanName}`;
        const cmd = new PutObjectCommand({
          Bucket: BUCKET,
          Key: key,
          ContentType: f.type || "application/octet-stream",
          // ACL: "private", // default
        });
        const url = await getSignedUrl(s3, cmd, { expiresIn: 900 }); // 15 minutes
        return { name: f.name, key, url };
      })
    );

    return res.status(200).json({ ok: true, urls });
  } catch (e: any) {
    console.error("presign error:", e);
    return res.status(500).json({ error: e?.message || "Failed to create presigned URLs" });
  }
}
