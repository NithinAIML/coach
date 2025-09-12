
// import React, { useEffect, useMemo, useRef, useState } from 'react';
// import styles from './home.module.scss';
// import { putS3, presignFiles, uploadWithPresigned } from '@/utils/s3';

// /* ======================= Types & constants ======================= */
// type FormModel = {
//   teamName: string;
//   department: string;
//   domain: string;
//   contactEmail: string;
//   description: string;
// };
// type Section = 'onboard' | 'dashboard' | 'knowledge' | 'auto' | 'analytics' | 'settings';

// const DOMAINS = ['Select domain', 'Support', 'Engineering', 'Sales', 'HR', 'Finance'];
// const TOTAL_STEPS = 4;

// /* ============================ Icons ============================= */
// type IconProps = { size?: number };
// const Icon = {
//   Plus: ({ size = 18 }: IconProps) => (
//     <svg className={styles.icon} style={{ width: size, height: size }} viewBox="0 0 24 24" aria-hidden="true"><path d="M12 5v14M5 12h14"/></svg>
//   ),
//   ChartBars: ({ size = 18 }: IconProps) => (
//     <svg className={styles.icon} style={{ width: size, height: size }} viewBox="0 0 24 24" aria-hidden="true"><path d="M4 19h16M7 17V9M12 17V5M17 17v-6"/></svg>
//   ),
//   Database: ({ size = 18 }: IconProps) => (
//     <svg className={styles.icon} style={{ width: size, height: size }} viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7c0 1.7 3.6 3 8 3s8-1.3 8-3-3.6-3-8-3-8 1.3-8 3zm16 5c0 1.7-3.6 3-8 3s-8-1.3-8-3m16 5c0 1.7-3.6 3-8 3s-8-1.3-8-3V7"/></svg>
//   ),
//   Calendar: ({ size = 18 }: IconProps) => (
//     <svg className={styles.icon} style={{ width: size, height: size }} viewBox="0 0 24 24" aria-hidden="true"><path d="M7 3v3M17 3v3M4 9h16M6 5h12a2 2 0 0 1 2 2v11a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V7a2 2 0 0 1 2-2z"/></svg>
//   ),
//   TrendUp: ({ size = 18 }: IconProps) => (
//     <svg className={styles.icon} style={{ width: size, height: size }} viewBox="0 0 24 24" aria-hidden="true"><path d="M3 17l6-6 4 4 7-7M15 8h5v5"/></svg>
//   ),
//   Cog: ({ size = 18 }: IconProps) => (
//     <svg className={styles.icon} style={{ width: size, height: size }} viewBox="0 0 24 24" aria-hidden="true">
//       <path d="M12 8a4 4 0 1 1 0 8 4 4 0 0 1 0-8z"/>
//       <path d="M12 2v3M12 19v3M4.6 5.6l2.1 2.1M17.3 18.3l2.1 2.1M2 12h3M19 12h3M4.6 18.4l2.1-2.1M17.3 5.7l2.1-2.1"/>
//     </svg>
//   ),
//   File: ({ size = 18 }: IconProps) => (
//     <svg className={styles.icon} style={{ width: size, height: size }} viewBox="0 0 24 24" aria-hidden="true">
//       <path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V9z"/><path d="M14 3v6h6"/>
//     </svg>
//   ),
//   Upload: ({ size = 18 }: IconProps) => (
//     <svg className={styles.icon} style={{ width: size, height: size }} viewBox="0 0 24 24" aria-hidden="true">
//       <path d="M12 16V6"/><path d="M8 10l4-4 4 4"/><path d="M5 20h14"/>
//     </svg>
//   ),
//   Globe: ({ size = 18 }: IconProps) => (
//     <svg className={styles.icon} style={{ width: size, height: size }} viewBox="0 0 24 24" aria-hidden="true">
//       <path d="M12 21a9 9 0 1 0 0-18 9 9 0 0 0 0 18z"/><path d="M2.5 12h19"/><path d="M12 2.5c3 3.5 3 15.5 0 19"/><path d="M12 2.5c-3 3.5-3 15.5 0 19"/>
//     </svg>
//   ),
//   Cloud: ({ size = 18 }: IconProps) => (
//     <svg className={styles.icon} style={{ width: size, height: size }} viewBox="0 0 24 24" aria-hidden="true"><path d="M20 17a4 4 0 0 0-3.5-3.96A5 5 0 0 0 7 12a4 4 0 0 0-1 7.87h12A4 4 0 0 0 20 17z"/></svg>
//   ),
//   Confluence: ({ size = 18 }: IconProps) => (
//     <svg className={styles.icon} style={{ width: size, height: size }} viewBox="0 0 24 24" aria-hidden="true">
//       <path d="M6 4h9l3 3v13a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2z"/><path d="M15 4v4h4"/><path d="M8 11h8M8 15h8"/>
//     </svg>
//   ),
//   Check: ({ size = 16 }: IconProps) => (
//     <svg viewBox="0 0 24 24" aria-hidden="true" style={{ width: size, height: size }}>
//       <path d="M20 6L9 17l-5-5" stroke="currentColor" strokeWidth="3" fill="none" strokeLinecap="round" strokeLinejoin="round"/>
//     </svg>
//   ),
//   Bolt: ({ size = 14 }: IconProps) => (
//     <svg viewBox="0 0 24 24" aria-hidden="true" style={{ width: size, height: size }}>
//       <path d="M13 2L3 14h7l-1 8 10-12h-7l1-8z" fill="currentColor"/>
//     </svg>
//   ),
//   Spark: ({ size = 14 }: IconProps) => (
//     <svg viewBox="0 0 24 24" aria-hidden="true" style={{ width: size, height: size }}>
//       <path d="M12 2l2 6 6 2-6 2-2 6-2-6-6-2 6-2z" fill="currentColor"/>
//     </svg>
//   ),
//   Key: ({ size = 16 }: IconProps) => (
//     <svg viewBox="0 0 24 24" style={{ width: size, height: size }}>
//       <path d="M21 7a5 5 0 1 1-9.8 1H3v4h5v4h4v-4h1.2A5 5 0 0 1 21 7z" fill="currentColor"/>
//     </svg>
//   ),
//   Moon: ({ size = 16 }: IconProps) => (
//     <svg viewBox="0 0 24 24" style={{ width: size, height: size }}>
//       <path d="M21 12.8A9 9 0 1 1 11.2 3 7 7 0 0 0 21 12.8z" fill="currentColor"/>
//     </svg>
//   ),
//   Sun: ({ size = 16 }: IconProps) => (
//     <svg viewBox="0 0 24 24" style={{ width: size, height: size }}>
//       <circle cx="12" cy="12" r="4" fill="currentColor"/><path d="M12 2v3M12 19v3M4.6 4.6l2.1 2.1M17.3 17.3l2.1 2.1M2 12h3M19 12h3M4.6 19.4l2.1-2.1M17.3 6.7l2.1-2.1" stroke="currentColor" fill="none"/>
//     </svg>
//   ),
// };

// /* =========================== Stepper ============================ */
// const Stepper: React.FC<{ active: number; total: number }> = ({ active, total }) => {
//   const steps = Array.from({ length: total }, (_, i) => i + 1);
//   return (
//     <div className={styles.stepper} role="progressbar" aria-valuemin={1} aria-valuemax={total} aria-valuenow={active}>
//       {steps.map((s, i) => {
//         const isComplete = s < active;
//         const isActive = s === active;
//         const base = `${styles.step} ${!isComplete && !isActive ? styles.stepInactive : ''}`;
//         const style: React.CSSProperties = isComplete || isActive ? { background: 'var(--primary)', color: '#fff' } : {};
//         return (
//           <React.Fragment key={s}>
//             <div className={base} style={style}>{isComplete ? <Icon.Check/> : s}</div>
//             {i < steps.length - 1 && <div className={`${styles.connector} ${s < active ? styles.connectorActive : ''}`}/>}
//           </React.Fragment>
//         );
//       })}
//     </div>
//   );
// };

// /* =========================== Helpers ============================ */
// const Title: React.FC<{ children: React.ReactNode }> = ({ children }) => (
//   <div style={{ fontWeight: 600, fontSize: 16, margin: '2px 0 12px' }}>{children}</div>
// );
// const Card: React.FC<{ children: React.ReactNode; pad?: number; style?: React.CSSProperties }> = ({ children, pad = 18, style }) => (
//   <div className={styles.card} style={{ padding: pad, borderRadius: 12, width: '100%', boxSizing: 'border-box', overflow: 'hidden', ...style }}>{children}</div>
// );
// const Toolbar: React.FC<{ left?: React.ReactNode; right?: React.ReactNode }> = ({ left, right }) => (
//   <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12, marginBottom: 10 }}>
//     <div>{left}</div>
//     <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>{right}</div>
//   </div>
// );

// /* ========================== Component =========================== */
// const SelfServicePortal: React.FC = () => {
//   const [section, setSection] = useState<Section>('dashboard');

//   /* Wizard state, Step 1–4 */
//   const [activeStep, setActiveStep] = useState<number>(1);

//   const [form, setForm] = useState<FormModel>({ teamName: '', department: '', domain: '', contactEmail: '', description: '' });
//   const [touched, setTouched] = useState<Record<keyof FormModel, boolean>>({ teamName: false, department: false, domain: false, contactEmail: false, description: false });
//   const errors = useMemo(() => {
//     const e: Partial<Record<keyof FormModel, string>> = {};
//     if (!form.teamName.trim()) e.teamName = 'Team name is required';
//     if (!form.department.trim()) e.department = 'Department is required';
//     if (!form.domain.trim()) e.domain = 'Please select a domain';
//     if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(form.contactEmail)) e.contactEmail = 'Valid email required';
//     return e;
//   }, [form]);
//   const isStep1Valid = Object.keys(errors).length === 0;

//   // Step-1 submission states
//   const [savedStep1, setSavedStep1] = useState(false);
//   const [savingStep1, setSavingStep1] = useState(false);
//   const [saveErr1, setSaveErr1] = useState<string | null>(null);
//   const [saveOk1, setSaveOk1] = useState<string | null>(null);
//   const step1Locked = savedStep1;

//   // Step-2 submission states
//   const [sources, setSources] = useState<string[]>([]);
//   const [selectedSource, setSelectedSource] = useState<string | null>(null);
//   const [confluence, setConfluence] = useState({ name: '', url: '', description: '', autoRefresh: true, frequency: 'Weekly', time: '09:00' });

//   const [fileUpload, setFileUpload] = useState<{
//     name: string; description: string; autoRefresh: boolean; frequency: 'Daily'|'Weekly'|'Monthly'; time: string; files: File[];
//   }>({ name: '', description: '', autoRefresh: true, frequency: 'Weekly', time: '09:00', files: [] });

//   const [savingStep2, setSavingStep2] = useState(false);
//   const [saveErr2, setSaveErr2] = useState<string | null>(null);
//   const [saveOk2, setSaveOk2] = useState<string | null>(null);
//   const [step2Saved, setStep2Saved] = useState(false);
//   const step2Locked = step2Saved;
//   const [uploadStatuses, setUploadStatuses] = useState<Record<string, 'idle'|'uploading'|'done'|'error'>>({});

//   const fileInputRef = useRef<HTMLInputElement | null>(null);
//   const onDrop: React.DragEventHandler<HTMLDivElement> = (e) => { e.preventDefault(); const f = Array.from(e.dataTransfer.files || []); if (f.length) setFileUpload(p => ({ ...p, files: [...p.files, ...f] })); };
//   const onDragOver: React.DragEventHandler<HTMLDivElement> = (e) => e.preventDefault();
//   const onFilePick: React.ChangeEventHandler<HTMLInputElement> = (e) => { const f = Array.from(e.target.files || []); if (f.length) setFileUpload(p => ({ ...p, files: [...p.files, ...f] })); };
//   const removeFile = (i: number) => setFileUpload(p => ({ ...p, files: p.files.filter((_, idx) => idx !== i) }));

//   const [processing, setProcessing] = useState(false);
//   const [processed, setProcessed] = useState(false);

//   const [testQuestions, setTestQuestions] = useState<string[]>(['']);
//   const [testing, setTesting] = useState(false);
//   const [deployed, setDeployed] = useState(false);
//   const [deployToCrewMate, setDeployToCrewMate] = useState(true);
//   const sourcesProcessed = sources.length;
//   const chunksCreated = 800 + sourcesProcessed * 200 + fileUpload.files.length * 15;
//   const qualityScore = 98;

//   // Continue button gating:
//   const canContinue =
//     activeStep === 1 ? savedStep1 :
//     activeStep === 2 ? step2Saved && !savingStep2 :
//     activeStep === 3 ? processed && !processing :
//     true;

//   function goNext() {
//     if (activeStep === TOTAL_STEPS) return;
//     if (activeStep === 1) {
//       setTouched({ teamName: true, department: true, domain: true, contactEmail: true, description: true });
//       if (!isStep1Valid || !savedStep1) return; // must submit successfully first
//     }
//     setActiveStep(s => Math.min(TOTAL_STEPS, s + 1));
//     window.scrollTo({ top: 0, behavior: 'smooth' });
//   }
//   function goPrev() {
//     if (activeStep === 1) return;
//     setActiveStep(s => Math.max(1, s - 1));
//     window.scrollTo({ top: 0, behavior: 'smooth' });
//   }

//   // On load: if we have a previously saved email, check registration to skip onboarding
//   useEffect(() => {
//     const cached = localStorage.getItem('coach.registration');
//     if (!cached) return;
//     try {
//       const { contactEmail } = JSON.parse(cached) || {};
//       if (!contactEmail) return;
//       fetch(`/api/registration-status?email=${encodeURIComponent(contactEmail)}`)
//         .then(r => r.ok ? r.json() : Promise.resolve({ registered: false }))
//         .then((d) => {
//           if (d?.registered) {
//             setSavedStep1(true);
//             setSection('dashboard');
//           }
//         })
//         .catch(() => {});
//     } catch { /* noop */ }
//   }, []);

//   // Fallback check by cookies (if no localStorage)
//   useEffect(() => {
//     const cached = localStorage.getItem('coach.registration');
//     if (cached) return;
//     fetch('/api/registration-status')
//       .then(r => r.ok ? r.json() : Promise.resolve({ registered: false }))
//       .then(d => { if (d?.registered) { setSavedStep1(true); setSection('dashboard'); } })
//       .catch(() => {});
//   }, []);

//   // Step-1: submit to S3 (enables Continue on success)
//   async function submitTeamRegistration() {
//     setSaveErr1(null);
//     setSaveOk1(null);
//     if (!isStep1Valid) {
//       setTouched({ teamName: true, department: true, domain: true, contactEmail: true, description: true });
//       return;
//     }
//     try {
//       setSavingStep1(true);
//       const payload = { kind: 'registration', ...form, savedAt: new Date().toISOString() };
//       const res = await putS3(payload);
//       const data = await res.json().catch(() => ({} as any));

//       if (res?.ok) {
//         setSavedStep1(true);
//         setSaveOk1(data?.message || 'Team details saved successfully.');
//         localStorage.setItem('coach.registration', JSON.stringify({ contactEmail: form.contactEmail, teamName: form.teamName }));
//       } else {
//         throw new Error(data?.error || 'Registration failed');
//       }
//     } catch (e: any) {
//       setSaveErr1(e?.message || 'Failed to register team');
//     } finally {
//       setSavingStep1(false);
//     }
//   }

//   // Step-2: SUBMIT (from either Confluence or File Upload)
//   async function submitStep2Sources() {
//     setSaveErr2(null);
//     setSaveOk2(null);
//     setSavingStep2(true);

//     const files = fileUpload.files;
//     const uploaded: Array<{ name: string; key: string }> = [];

//     try {
//       // Upload files (if any)
//       if (files.length > 0) {
//         const st: Record<string, 'idle'|'uploading'|'done'|'error'> = {};
//         files.forEach((f) => (st[f.name] = 'idle'));
//         setUploadStatuses(st);

//         const presigned = await presignFiles(form.contactEmail, files);

//         for (const p of presigned) {
//           const f = files.find(ff => ff.name === p.name);
//           if (!f) { setUploadStatuses(s => ({ ...s, [p.name]: 'error' })); throw new Error(`Missing file ${p.name}`); }
//           setUploadStatuses(s => ({ ...s, [p.name]: 'uploading' }));
//           await uploadWithPresigned(p, f);
//           setUploadStatuses(s => ({ ...s, [p.name]: 'done' }));
//           uploaded.push({ name: p.name, key: p.key });
//         }
//       }

//       // Save knowledge sources snapshot
//       const payload = {
//         kind: 'sources',
//         contactEmail: form.contactEmail,
//         selected: sources,
//         confluence,
//         fileUpload: {
//           ...fileUpload,
//           filesMeta: files.map((f) => ({ name: f.name, size: f.size, type: (f as any).type || undefined })),
//           filesS3: uploaded,
//         },
//         savedAt: new Date().toISOString(),
//       };
//       const res = await putS3(payload);
//       const data = await res.json().catch(() => ({} as any));
//       if (!res?.ok) throw new Error(data?.error || 'Failed to save knowledge sources');

//       // Lock Step-2 and show green banner
//       setStep2Saved(true);
//       setSaveOk2('Knowledge sources saved successfully. You can proceed to processing.');
//     } catch (e: any) {
//       setSaveErr2(e?.message || 'Failed to save knowledge sources');
//     } finally {
//       setSavingStep2(false);
//     }
//   }

//   /* -------------------------- Dashboard -------------------------- */
//   const RowStat: React.FC<{ label: string; value: string; delta: string; positive?: boolean; icon: React.ReactNode }> =
//     ({ label, value, delta, positive = true, icon }) => (
//       <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0,1fr) auto', alignItems: 'center', padding: '14px 16px', border: '1px solid var(--line)', borderRadius: 10, background: '#fff' }}>
//         <div>
//           <div style={{ fontSize: 12, color: 'var(--muted)', marginBottom: 6 }}>{label}</div>
//           <div style={{ fontSize: 22, fontWeight: 700 }}>{value}</div>
//           <div style={{ fontSize: 12, color: positive ? '#16a34a' : '#ef4444', marginTop: 6 }}>{positive ? '↑ ' : '↓ '}{delta}</div>
//         </div>
//         <div style={{ width: 28, height: 28, borderRadius: 8, display: 'grid', placeItems: 'center', color: 'var(--primary)', background: '#f3f4f6' }}>{icon}</div>
//       </div>
//     );

//   const DashboardView = (
//     <div style={{ display: 'grid', gap: 16 }}>
//       <Card pad={16}>
//         <div style={{ display: 'grid', gap: 12 }}>
//           <RowStat label="Total queries" value="1,247" delta="12% from last week" icon={<Icon.TrendUp size={14} />} />
//           <RowStat label="Avg response time" value="0.8s" delta="0.2s from last week" positive={false} icon={<Icon.Bolt size={14} />} />
//           <RowStat label="Satisfaction rate" value="94%" delta="3% from last week" icon={<Icon.Spark size={14} />} />
//           <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0,1fr) auto', alignItems: 'center', padding: '14px 16px', border: '1px solid var(--line)', borderRadius: 10, background: '#fff' }}>
//             <div>
//               <div style={{ fontSize: 12, color: 'var(--muted)', marginBottom: 6 }}>Knowledge gaps</div>
//               <div style={{ fontSize: 22, fontWeight: 700 }}>12</div>
//               <div style={{ marginTop: 6 }}><span style={{ padding: '2px 8px', borderRadius: 999, background: '#fff7ed', color: '#9a3412', fontSize: 12, fontWeight: 600 }}>Needs attention</span></div>
//             </div>
//             <div style={{ width: 28, height: 28, borderRadius: 8, display: 'grid', placeItems: 'center', background: '#fff7ed', color: '#f97316' }}>!</div>
//           </div>
//         </div>
//       </Card>
//       <Card pad={16}>
//         <div style={{ fontWeight: 600, marginBottom: 12 }}>Recent activity</div>
//         <div style={{ display: 'grid', gap: 10 }}>
//           {[
//             { dot: 'var(--primary)', text: 'How to configure MFA?', time: '2 mins ago' },
//             { dot: '#10b981', text: 'Confluence space updated', time: '1 hour ago' },
//             { dot: '#6366f1', text: 'Feedback: “Very helpful response”', time: '3 hours ago' },
//           ].map((i, idx) => (
//             <div key={idx} style={{ display: 'grid', gridTemplateColumns: 'auto minmax(0,1fr) auto', gap: 10, alignItems: 'center', padding: '10px 8px', border: '1px solid var(--line)', borderRadius: 8, background: '#fff' }}>
//               <span style={{ width: 8, height: 8, background: i.dot, borderRadius: 999 }} />
//               <span style={{ minWidth: 0 }}>{i.text}</span>
//               <span style={{ color: 'var(--muted)', fontSize: 12 }}>{i.time}</span>
//             </div>
//           ))}
//         </div>
//       </Card>
//       <Card pad={16}>
//         <div style={{ fontWeight: 600, marginBottom: 12 }}>Popular topics</div>
//         <div style={{ display: 'grid', gap: 10, maxWidth: 720 }}>
//           {[
//             { label: 'Access management', pct: 68 },
//             { label: 'Policy configuration', pct: 70 },
//             { label: 'Troubleshooting', pct: 56 },
//           ].map((t) => (
//             <div key={t.label} style={{ display: 'grid', gridTemplateColumns: 'minmax(0,1fr) 120px', gap: 16, alignItems: 'center' }}>
//               <div style={{ color: '#111827' }}>{t.label}</div>
//               <div>
//                 <div style={{ height: 8, background: '#e5e7eb', borderRadius: 999 }}>
//                   <div style={{ width: `${t.pct}%`, height: 8, background: 'var(--primary)', borderRadius: 999 }} />
//                 </div>
//                 <div style={{ fontSize: 12, color: 'var(--muted)', textAlign: 'right', marginTop: 4 }}>{t.pct}%</div>
//               </div>
//             </div>
//           ))}
//         </div>
//       </Card>
//     </div>
//   );

//   /* --------------------- Knowledge Sources tab -------------------- */
//   const KnowledgeSourcesView = (
//     <div className={styles.cardWrap}>
//       <Toolbar
//         left={<h2 className={styles.cardTitle} style={{ margin: 0, fontWeight: 600 }}>Knowledge sources</h2>}
//         right={
//           <>
//             <button type="button" className={styles.btn} style={{ background: 'var(--success)', color: '#fff' }}>⟳ Sync all</button>
//             <button type="button" className={`${styles.btn} ${styles.btnPrimary}`} onClick={() => { setSection('onboard'); setActiveStep(2); }}>+ Add source</button>
//           </>
//         }
//       />
//       <Card pad={22}>
//         <div className={styles.emptyState} style={{ paddingTop: 30, paddingBottom: 18 }}>
//           <div className={styles.emptyIcon}><Icon.Database size={28} /></div>
//           <p className={styles.emptyText} style={{ marginBottom: 6 }}>No knowledge sources configured yet.</p>
//           <button type="button" onClick={() => { setSection('onboard'); setActiveStep(2); }} style={{ appearance: 'none', background: 'none', border: 'none', color: 'var(--primary)', cursor: 'pointer', fontWeight: 600 }}>
//             Add your first knowledge source →
//           </button>
//         </div>
//       </Card>
//     </div>
//   );

//   /* --------------------- Auto-Refresh (FIXED) --------------------- */
//   type SchedRow = { name: string; freq: 'Daily'|'Weekly'|'Monthly'; next: string; active: boolean };
//   const [autoEnabled, setAutoEnabled] = useState(true);
//   const [timezone, setTimezone] = useState('America/New_York');
//   const [maintFrom, setMaintFrom] = useState('02:00');
//   const [maintTo, setMaintTo] = useState('04:00');
//   const [retries, setRetries] = useState(3);
//   const [rows, setRows] = useState<SchedRow[]>([
//     { name: 'IAM Confluence Space', freq: 'Daily',  next: '2025-08-02 09:00', active: true },
//     { name: 'Security Policies',     freq: 'Weekly', next: '2025-08-03 06:00', active: true },
//   ]);
//   const tzOptions = ['America/New_York','America/Chicago','America/Los_Angeles','Europe/London','Europe/Berlin','Asia/Singapore','Asia/Kolkata'];

//   const fullWidthControl: React.CSSProperties = { width: '100%', maxWidth: '100%', boxSizing: 'border-box', minWidth: 0 };

//   const AutoRefreshView = (
//     <div className={styles.cardWrap} style={{ display: 'grid', gap: 16 }}>
//       <Toolbar
//         left={<h2 className={styles.cardTitle} style={{ margin: 0, fontWeight: 600 }}>Auto-refresh scheduler</h2>}
//         right={
//           <>
//             <span style={{
//               display: 'inline-flex', alignItems: 'center', gap: 8,
//               background: autoEnabled ? '#e8faf0' : '#fff7ed',
//               color: autoEnabled ? '#166534' : '#9a3412',
//               padding: '6px 10px', borderRadius: 999, fontSize: 12, fontWeight: 600
//             }}>
//               <i className={styles.statusDot} /> {autoEnabled ? 'Auto-refresh enabled' : 'Auto-refresh disabled'}
//             </span>
//             <button type="button" className={styles.btn} style={{ background: '#fecaca', color: '#7f1d1d', border: '1px solid #fca5a5' }} onClick={() => { setAutoEnabled(false); setRows(r => r.map(x => ({ ...x, active: false }))); }}>
//               Disable all
//             </button>
//           </>
//         }
//       />

//       {/* Global settings — force minWidth:0 + fullWidth controls */}
//       <Card pad={18}>
//         <div style={{ minWidth: 0 }}>
//           <Title>Global refresh settings</Title>
//           <div className={styles.formGrid} style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: 12, minWidth: 0 }}>
//             <div style={{ minWidth: 0 }}>
//               <label className={styles.label} htmlFor="tz">Timezone</label>
//               <select id="tz" className={styles.select} value={timezone} onChange={(e) => setTimezone(e.target.value)} disabled={!autoEnabled} style={fullWidthControl}>
//                 {tzOptions.map(tz => <option key={tz} value={tz}>{tz}</option>)}
//               </select>
//             </div>
//             <div style={{ minWidth: 0 }}>
//               <label className={styles.label}>Maintenance window</label>
//               <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0,1fr) auto minmax(0,1fr)', gap: 8, alignItems: 'center', minWidth: 0 }}>
//                 <input type="time" className={styles.input} value={maintFrom} onChange={(e) => setMaintFrom(e.target.value)} disabled={!autoEnabled} style={fullWidthControl} />
//                 <span style={{ color: 'var(--muted)', fontSize: 12, textAlign: 'center' }}>to</span>
//                 <input type="time" className={styles.input} value={maintTo} onChange={(e) => setMaintTo(e.target.value)} disabled={!autoEnabled} style={fullWidthControl} />
//               </div>
//             </div>
//             <div style={{ minWidth: 0 }}>
//               <label className={styles.label} htmlFor="retry">Retry attempts</label>
//               <input id="retry" type="number" min={0} className={styles.input} value={retries} onChange={(e) => setRetries(parseInt(e.target.value || '0', 10))} disabled={!autoEnabled} style={fullWidthControl} />
//             </div>
//           </div>
//         </div>
//       </Card>

//       {/* Scheduled updates */}
//       <Card pad={18}>
//         <Title>Scheduled updates</Title>
//         <div style={{ overflowX: 'auto' }}>
//           <table style={{ width: '100%', borderCollapse: 'collapse' }}>
//             <thead>
//               <tr style={{ textAlign: 'left', color: 'var(--muted)', fontSize: 12 }}>
//                 <th style={{ padding: '12px 8px' }}>Knowledge source</th>
//                 <th style={{ padding: '12px 8px' }}>Frequency</th>
//                 <th style={{ padding: '12px 8px' }}>Next run</th>
//                 <th style={{ padding: '12px 8px' }}>Status</th>
//               </tr>
//             </thead>
//             <tbody>
//               {rows.map((r, i) => (
//                 <tr key={i} style={{ borderTop: '1px solid var(--line)', background: '#fff' }}>
//                   <td style={{ padding: '14px 8px' }}><span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}><Icon.Confluence />{r.name}</span></td>
//                   <td style={{ padding: '14px 8px' }}><span style={{ background: '#eef2ff', padding: '2px 8px', borderRadius: 999, fontSize: 12 }}>{r.freq}</span></td>
//                   <td style={{ padding: '14px 8px' }}>{r.next}</td>
//                   <td style={{ padding: '14px 8px' }}>
//                     <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6, color: r.active ? '#166534' : '#9a3412' }}>
//                       <i className={styles.statusDot} /> {r.active ? 'Active' : 'Paused'}
//                     </span>
//                   </td>
//                 </tr>
//               ))}
//             </tbody>
//           </table>
//         </div>
//       </Card>
//     </div>
//   );

//   /* ------------------------- Analytics tab ------------------------ */
//   const querySeries = [120, 160, 180, 220, 260, 240, 280];
//   const rtSeries = [1.1, 0.9, 0.85, 0.82, 0.78, 0.81, 0.8];
//   const Sparkline: React.FC<{ data: number[]; height?: number; max?: number }> = ({ data, height = 40, max }) => {
//     const w = 180; const h = height; const m = max ?? Math.max(...data) * 1.1;
//     const step = w / (data.length - 1);
//     const pts = data.map((v, i) => [i * step, h - (v / m) * (h - 2)] as const);
//     const d = pts.map((p, i) => (i ? `L${p[0]},${p[1]}` : `M${p[0]},${p[1]}`)).join(' ');
//     return <svg width={w} height={h} viewBox={`0 0 ${w} ${h}`} aria-hidden="true"><path d={d} fill="none" stroke="currentColor" strokeWidth="2" /></svg>;
//   };
//   const Donut: React.FC<{ pct: number; size?: number }> = ({ pct, size = 90 }) => {
//     const r = 34; const c = 2 * Math.PI * r; const off = c * (1 - pct / 100);
//     return (
//       <svg width={size} height={size} viewBox="0 0 84 84" aria-hidden="true">
//         <circle cx="42" cy="42" r={r} stroke="#e5e7eb" strokeWidth="10" fill="none" />
//         <circle cx="42" cy="42" r={r} stroke="currentColor" strokeWidth="10" fill="none" strokeDasharray={c} strokeDashoffset={off} strokeLinecap="round" />
//         <text x="42" y="47" textAnchor="middle" fontSize="16" fontWeight={700}>{pct}%</text>
//       </svg>
//     );
//   };
//   const AnalyticsRow: React.FC<{ title: string; left: React.ReactNode; right?: React.ReactNode }> = ({ title, left, right }) => (
//     <Card pad={16}>
//       <div style={{ fontWeight: 600, marginBottom: 12 }}>{title}</div>
//       <div style={{ display: 'grid', gridTemplateColumns: right ? 'minmax(0,1fr) auto' : 'minmax(0,1fr)', gap: 16, alignItems: 'center' }}>
//         <div>{left}</div>
//         {right}
//       </div>
//     </Card>
//   );
//   const AnalyticsView = (
//     <div style={{ display: 'grid', gap: 16 }}>
//       <AnalyticsRow title="Total queries (7d)" left={<div><div style={{ fontSize: 28, fontWeight: 700 }}>1,904</div><div style={{ color: '#16a34a', fontSize: 12, marginTop: 6 }}>↑ 14% vs last week</div></div>} right={<div style={{ color: 'var(--primary)' }}><Sparkline data={querySeries} /></div>} />
//       <AnalyticsRow title="Average response time" left={<div><div style={{ fontSize: 28, fontWeight: 700 }}>0.80s</div><div style={{ color: '#ef4444', fontSize: 12, marginTop: 6 }}>↓ 0.2s vs last week</div></div>} right={<div style={{ color: 'var(--primary)' }}><Sparkline data={rtSeries} max={1.2} /></div>} />
//       <Card pad={16}>
//         <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: 16 }}>
//           <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0,1fr) auto', alignItems: 'center', border: '1px solid var(--line)', borderRadius: 10, padding: '14px 16px', background: '#fff' }}>
//             <div><div style={{ color: 'var(--muted)', fontSize: 12, marginBottom: 6 }}>Satisfaction</div><div style={{ fontSize: 24, fontWeight: 700 }}>95%</div></div>
//             <div style={{ color: 'var(--primary)' }}><Donut pct={95} /></div>
//           </div>
//           <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0,1fr) auto', alignItems: 'center', border: '1px solid var(--line)', borderRadius: 10, padding: '14px 16px', background: '#fff' }}>
//             <div><div style={{ color: 'var(--muted)', fontSize: 12, marginBottom: 6 }}>Deflection rate</div><div style={{ fontSize: 24, fontWeight: 700 }}>62%</div></div>
//             <div style={{ color: 'var(--primary)' }}><Donut pct={62} /></div>
//           </div>
//         </div>
//       </Card>
//       <Card pad={16}>
//         <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0,1fr) minmax(0,1fr)', gap: 16 }}>
//           <div>
//             <Title>Channel breakdown</Title>
//             {[
//               { label: 'Portal', pct: 48 },
//               { label: 'Slack',  pct: 32 },
//               { label: 'Email',  pct: 14 },
//               { label: 'Other',  pct: 6  },
//             ].map((it) => (
//               <div key={it.label} style={{ marginBottom: 12 }}>
//                 <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 13, marginBottom: 6 }}>
//                   <span>{it.label}</span><span style={{ color: 'var(--muted)' }}>{it.pct}%</span>
//                 </div>
//                 <div style={{ height: 10, background: '#e5e7eb', borderRadius: 999 }}>
//                   <div style={{ width: `${it.pct}%`, height: 10, background: 'var(--primary)', borderRadius: 999 }} />
//                 </div>
//               </div>
//             ))}
//           </div>
//           <div>
//             <Title>Top teams by usage</Title>
//             <div style={{ display: 'grid', gap: 10 }}>
//               {[
//                 { team: 'Support',      q: 640 },
//                 { team: 'Engineering',  q: 520 },
//                 { team: 'Sales',        q: 310 },
//                 { team: 'Finance',      q: 214 },
//               ].map((r) => (
//                 <div key={r.team} style={{ display: 'grid', gridTemplateColumns: 'minmax(0,1fr) auto', gap: 8, alignItems: 'center', padding: '12px 10px', background: '#fff', border: '1px solid var(--line)', borderRadius: 8 }}>
//                   <span>{r.team}</span>
//                   <span style={{ color: 'var(--muted)' }}>{r.q.toLocaleString()} queries</span>
//                 </div>
//               ))}
//             </div>
//           </div>
//         </div>
//       </Card>
//     </div>
//   );

//   /* ---------------------- Settings (FIXED) ----------------------- */
//   const [orgName, setOrgName] = useState('Vg Corp');
//   const [logoUrl, setLogoUrl] = useState('');
//   const [theme, setTheme] = useState<'light' | 'dark'>('light');
//   const [defaultTZ, setDefaultTZ] = useState('America/New_York');
//   const [retentionDays, setRetentionDays] = useState(365);
//   const [apiKeyVisible, setApiKeyVisible] = useState(false);
//   const [apiKey] = useState('sk-live-************************-abcd');

//   type Member = { name: string; email: string; role: 'Admin' | 'Editor' | 'Viewer'; active: boolean };
//   const [members, setMembers] = useState<Member[]>([
//     { name: 'Nithin',  email: 'nithin@gmail.com',  role: 'Admin',  active: true },
//     { name: 'Eric',    email: 'eric@gmail.com',    role: 'Editor', active: true },
//     { name: 'Swaroop', email: 'swaroop@gmail.com', role: 'Viewer', active: true },
//   ]);
//   const setRole   = (i: number, role: Member['role']) => setMembers(ms => ms.map((m, idx) => (idx === i ? { ...m, role } : m)));
//   const toggleActive = (i: number) => setMembers(ms => ms.map((m, idx) => (idx === i ? { ...m, active: !m.active } : m)));

//   const SettingsView = (
//     <div style={{ display: 'grid', gap: 16 }}>
//       <Card pad={18}>
//         <div style={{ minWidth: 0 }}>
//           <Title>Organization</Title>
//           <div className={styles.formGrid} style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: 12, minWidth: 0 }}>
//             <div style={{ minWidth: 0 }}>
//               <label className={styles.label} htmlFor="org">Organization name</label>
//               <input id="org" className={styles.input} value={orgName} onChange={e => setOrgName(e.target.value)} style={fullWidthControl} />
//             </div>
//             <div style={{ minWidth: 0 }}>
//               <label className={styles.label} htmlFor="logo">Logo URL (optional)</label>
//               <input id="logo" className={styles.input} placeholder="https://…" value={logoUrl} onChange={e => setLogoUrl(e.target.value)} style={fullWidthControl} />
//             </div>
//           </div>
//         </div>
//       </Card>

//       <Card pad={18}>
//         <div style={{ minWidth: 0 }}>
//           <Title>Preferences</Title>
//           <div className={styles.formGrid} style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 12, minWidth: 0 }}>
//             <div style={{ minWidth: 0 }}>
//               <label className={styles.label}>Theme</label>
//               <div style={{ display: 'flex', gap: 10 }}>
//                 <button type="button" className={styles.btn} onClick={() => setTheme('light')} style={{ width: '100%', justifyContent: 'center', background: theme === 'light' ? 'var(--primary)' : '#fff', color: theme === 'light' ? '#fff' : '#374151' }}><Icon.Sun />&nbsp;Light</button>
//                 <button type="button" className={styles.btn} onClick={() => setTheme('dark')}  style={{ width: '100%', justifyContent: 'center', background: theme === 'dark' ? 'var(--primary)' : '#fff',  color: theme === 'dark'  ? '#fff' : '#374151' }}><Icon.Moon />&nbsp;Dark</button>
//               </div>
//             </div>
//             <div style={{ minWidth: 0 }}>
//               <label className={styles.label} htmlFor="dtz">Default timezone</label>
//               <select id="dtz" className={styles.select} value={defaultTZ} onChange={(e) => setDefaultTZ(e.target.value)} style={fullWidthControl}>
//                 <option>America/New_York</option><option>America/Chicago</option><option>America/Los_Angeles</option>
//                 <option>Europe/London</option><option>Europe/Berlin</option><option>Asia/Singapore</option><option>Asia/Kolkata</option>
//               </select>
//             </div>
//             <div style={{ minWidth: 0 }}>
//               <label className={styles.label} htmlFor="ret">Data retention (days)</label>
//               <input id="ret" type="number" min={30} className={styles.input} value={retentionDays} onChange={e => setRetentionDays(parseInt(e.target.value || '30', 10))} style={fullWidthControl} />
//             </div>
//           </div>
//         </div>
//       </Card>

//       <Card pad={18}>
//         <div style={{ minWidth: 0 }}>
//           <Title>API access</Title>
//           <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0,1fr) auto', gap: 12, alignItems: 'center', minWidth: 0 }}>
//             <div style={{ display: 'flex', gap: 10, alignItems: 'center', minWidth: 0 }}>
//               <span style={{ width: 28, height: 28, borderRadius: 8, display: 'grid', placeItems: 'center', background: '#f3f4f6', color: 'var(--primary)' }}><Icon.Key /></span>
//               <div style={{ minWidth: 0 }}>
//                 <div style={{ fontWeight: 600, marginBottom: 2 }}>Primary API key</div>
//                 <div style={{ color: 'var(--muted)', fontSize: 12 }}>Use for server-to-server integrations</div>
//               </div>
//             </div>
//             <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap', justifyContent: 'flex-end', minWidth: 0 }}>
//               <code style={{ background: '#f3f4f6', padding: '6px 8px', borderRadius: 6, whiteSpace: 'nowrap', maxWidth: 280, overflow: 'hidden', textOverflow: 'ellipsis' }} title={apiKey}>
//                 {apiKeyVisible ? apiKey : '••••••••••••••••••••••••••'}
//               </code>
//               <button type="button" className={styles.btn} onClick={() => setApiKeyVisible(v => !v)}>{apiKeyVisible ? 'Hide' : 'Reveal'}</button>
//               <button type="button" className={styles.btn} style={{ background: 'var(--success)', color: '#fff' }}>Rotate key</button>
//             </div>
//           </div>
//         </div>
//       </Card>

//       <Card pad={18}>
//         <Title>Members & access</Title>
//         <div style={{ overflowX: 'auto' }}>
//           <table style={{ width: '100%', borderCollapse: 'collapse' }}>
//             <thead>
//               <tr style={{ textAlign: 'left', color: 'var(--muted)', fontSize: 12 }}>
//                 <th style={{ padding: '10px 8px' }}>Name</th>
//                 <th style={{ padding: '10px 8px' }}>Email</th>
//                 <th style={{ padding: '10px 8px' }}>Role</th>
//                 <th style={{ padding: '10px 8px' }}>Status</th>
//               </tr>
//             </thead>
//             <tbody>
//               {members.map((m, i) => (
//                 <tr key={m.email} style={{ borderTop: '1px solid var(--line)' }}>
//                   <td style={{ padding: '12px 8px', whiteSpace: 'nowrap' }}>{m.name}</td>
//                   <td style={{ padding: '12px 8px', color: 'var(--muted)' }}>{m.email}</td>
//                   <td style={{ padding: '12px 8px' }}>
//                     <select className={styles.select} value={m.role} onChange={e => setRole(i, e.target.value as any)} style={{ width: 140 }}>
//                       <option>Admin</option><option>Editor</option><option>Viewer</option>
//                     </select>
//                   </td>
//                   <td style={{ padding: '12px 8px' }}>
//                     <button type="button" className={styles.btn} onClick={() => toggleActive(i)} style={{ background: m.active ? '#e8faf0' : '#fff7ed', color: m.active ? '#166534' : '#9a3412', width: 110, justifyContent: 'center' }}>
//                       {m.active ? 'Active' : 'Suspended'}
//                     </button>
//                   </td>
//                 </tr>
//               ))}
//             </tbody>
//           </table>
//         </div>
//       </Card>
//     </div>
//   );

//   /* -------------------------- Sub-nav ----------------------------- */
//   const SubnavLink: React.FC<{ id: Section; icon: React.ReactNode; label: string }> = ({ id, icon, label }) => (
//     <a href="#" className={`${styles.subnavLink} ${section === id ? styles.subnavLinkActive : ''}`} onClick={(e) => { e.preventDefault(); setSection(id); }}>
//       {icon}<span>{label}</span>
//     </a>
//   );

//   /* --------------------------- Render ---------------------------- */
//   const big = 26;
//   const dropzoneStyle: React.CSSProperties = { border: '2px dashed var(--line-2)', background: '#fff', borderRadius: 8, padding: 18, textAlign: 'center', color: 'var(--muted)', cursor: 'pointer' };
//   function addSource(kind: string) { if (step2Locked) return; setSources(p => (p.includes(kind) ? p : [...p, kind])); setSelectedSource(kind); }
//   function removeSource(kind: string) {
//     if (step2Locked) return;
//     setSources(prev => prev.filter(k => k !== kind));
//     if (selectedSource === kind) setSelectedSource(null);
//     if (kind === 'Confluence') setConfluence({ name: '', url: '', description: '', autoRefresh: true, frequency: 'Weekly', time: '09:00' });
//     if (kind === 'File Upload') setFileUpload({ name: '', description: '', autoRefresh: true, frequency: 'Weekly', time: '09:00', files: [] });
//   }

//   return (
//     <>
//       {/* App Bar */}
//       <header className={styles.appbar}>
//         <div className={`${styles.container} ${styles.appbarInner}`}>
//           <div className={styles.brand}>
//             <div className={styles.brandTitle}>COACH Self-Service Portal</div>
//             <div className={styles.brandSub}>AI-powered knowledge management platform</div>
//           </div>
//           <div className={styles.appbarRight}>
//             <span>
//               {section === 'onboard'   ? `Step ${activeStep} of ${TOTAL_STEPS}` :
//                section === 'dashboard' ? 'Dashboard' :
//                section === 'knowledge' ? 'Knowledge sources' :
//                section === 'auto'      ? 'Auto-refresh' :
//                section === 'analytics' ? 'Analytics' : 'Settings'}
//             </span>
//             <span><i className={styles.statusDot} />System online</span>
//           </div>
//         </div>
//       </header>

//       {/* Subnav */}
//       <nav className={styles.subnav}>
//         <div className={`${styles.container} ${styles.subnavInner}`}>
//           <SubnavLink id="onboard"   icon={<Icon.Plus />}      label="Onboard team" />
//           <SubnavLink id="dashboard" icon={<Icon.ChartBars />} label="Dashboard" />
//           <SubnavLink id="knowledge" icon={<Icon.Database />}  label="Knowledge sources" />
//           <SubnavLink id="auto"      icon={<Icon.Calendar />}  label="Auto-refresh" />
//           <SubnavLink id="analytics" icon={<Icon.TrendUp />}   label="Analytics" />
//           <SubnavLink id="settings"  icon={<Icon.Cog />}       label="Settings" />
//         </div>
//       </nav>

//       {/* Content */}
//       <main className={styles.page}>
//         <div className={styles.container} style={{ minWidth: 0 }}>
//           {section === 'dashboard' && DashboardView}
//           {section === 'knowledge'  && KnowledgeSourcesView}
//           {section === 'auto'       && AutoRefreshView}
//           {section === 'analytics'  && AnalyticsView}
//           {section === 'settings'   && SettingsView}

//           {section === 'onboard' && (
//             <>
//               <div className={styles.stepperWrap}><Stepper active={activeStep} total={TOTAL_STEPS} /></div>
//               <div className={styles.cardWrap}>
//                 <div className={styles.card}>
//                   <h2 className={styles.cardTitle} style={{ fontWeight: 600 }}>
//                     {activeStep === 1 ? 'Team registration' : activeStep === 2 ? 'Knowledge sources' : activeStep === 3 ? 'Processing knowledge' : 'Test & deploy'}
//                   </h2>
//                   {activeStep === 2 && <p className={styles.cardSubtext}>Add your team’s knowledge sources to create the AI knowledge base.</p>}
//                   {activeStep === 3 && <p className={styles.cardSubtext}>Ready to process your knowledge sources.</p>}

//                   {/* Step 1 */}
//                   {activeStep === 1 && (
//                     <form onSubmit={(e) => e.preventDefault()} noValidate>
//                       <div className={styles.formGrid}>
//                         <div>
//                           <label className={`${styles.label} ${styles.required}`} htmlFor="teamName">Team name</label>
//                           <input id="teamName" className={styles.input} placeholder="e.g., Cloud Support Team" value={form.teamName} onChange={e => setForm(p => ({ ...p, teamName: e.target.value }))} onBlur={() => setTouched(t => ({ ...t, teamName: true }))} aria-invalid={!!(touched.teamName && errors.teamName)} aria-describedby="err-teamName" disabled={step1Locked} />
//                           {touched.teamName && errors.teamName && <div id="err-teamName" className={styles.errorText}>{errors.teamName}</div>}
//                         </div>
//                         <div>
//                           <label className={`${styles.label} ${styles.required}`} htmlFor="department">Department</label>
//                           <input id="department" className={styles.input} placeholder="e.g., ESAF" value={form.department} onChange={e => setForm(p => ({ ...p, department: e.target.value }))} onBlur={() => setTouched(t => ({ ...t, department: true }))} aria-invalid={!!(touched.department && errors.department)} aria-describedby="err-dept" disabled={step1Locked} />
//                           {touched.department && errors.department && <div id="err-dept" className={styles.errorText}>{errors.department}</div>}
//                         </div>
//                         <div>
//                           <label className={`${styles.label} ${styles.required}`} htmlFor="domain">Domain</label>
//                           <select id="domain" className={styles.select} value={form.domain} onChange={e => setForm(p => ({ ...p, domain: e.target.value }))} onBlur={() => setTouched(t => ({ ...t, domain: true }))} disabled={step1Locked}>
//                             {DOMAINS.map((d, i) => <option value={i === 0 ? '' : d} key={d} disabled={i === 0}>{d}</option>)}
//                           </select>
//                           {touched.domain && errors.domain && <div className={styles.errorText}>{errors.domain}</div>}
//                         </div>
//                         <div>
//                           <label className={`${styles.label} ${styles.required}`} htmlFor="email">Contact email</label>
//                           <input id="email" className={styles.input} placeholder="team-lead@company.com" value={form.contactEmail} onChange={e => setForm(p => ({ ...p, contactEmail: e.target.value }))} onBlur={() => setTouched(t => ({ ...t, contactEmail: true }))} inputMode="email" autoComplete="email" aria-invalid={!!(touched.contactEmail && errors.contactEmail)} aria-describedby="err-email" disabled={step1Locked} />
//                           {touched.contactEmail && errors.contactEmail && <div id="err-email" className={styles.errorText}>{errors.contactEmail}</div>}
//                         </div>
//                         <div>
//                           <label className={styles.label} htmlFor="desc">Team description</label>
//                           <textarea id="desc" className={styles.textarea} placeholder="Brief description of your team's responsibilities" value={form.description} onChange={e => setForm(p => ({ ...p, description: e.target.value }))} onBlur={() => setTouched(t => ({ ...t, description: true }))} disabled={step1Locked} />
//                         </div>
//                       </div>

//                       {/* Step-1 submit & messages */}
//                       <div style={{ display: 'flex', gap: 10, marginTop: 12, flexWrap: 'wrap' }}>
//                         <button type="button" className={styles.btn} onClick={submitTeamRegistration} disabled={savingStep1 || !isStep1Valid || step1Locked} style={{ background: 'var(--primary)', color: '#fff' }}>
//                           {savingStep1 ? 'Submitting…' : (savedStep1 ? 'Submitted ✓' : 'Submit')}
//                         </button>
//                         {savedStep1 && (
//                           <span role="status" style={{ display: 'inline-flex', alignItems: 'center', gap: 8, background: '#e9fbe7', border: '1px solid #bbf7d0', color: '#14532d', padding: '8px 10px', borderRadius: 8 }}>
//                             <Icon.Check size={14} /> {saveOk1 || 'Team details saved successfully. You can continue.'}
//                           </span>
//                         )}
//                         {saveErr1 && !savedStep1 && (
//                           <span role="status" style={{ display: 'inline-flex', alignItems: 'center', gap: 8, background: '#fff7ed', border: '1px solid #fed7aa', color: '#9a3412', padding: '8px 10px', borderRadius: 8 }}>
//                             {saveErr1}
//                           </span>
//                         )}
//                       </div>
//                     </form>
//                   )}

//                   {/* Step 2 */}
//                   {activeStep === 2 && (
//                     <>
//                       {/* Success/Fail banners */}
//                       {saveOk2 && (
//                         <div role="status" style={{ background: '#e9fbe7', border: '1px solid #bbf7d0', color: '#14532d', padding: 12, borderRadius: 8, marginBottom: 10 }}>
//                           <strong>Success:</strong> {saveOk2}
//                         </div>
//                       )}
//                       {saveErr2 && (
//                         <div role="alert" style={{ background: '#fff7ed', border: '1px solid #fed7aa', color: '#9a3412', padding: 12, borderRadius: 8, marginBottom: 10 }}>
//                           {saveErr2}
//                         </div>
//                       )}

//                       <div className={styles.sourcesGrid} role="group" aria-label="Add knowledge source">
//                         <button type="button" className={`${styles.sourceBtn} ${selectedSource === 'Confluence' ? styles.sourceBtnActive : ''}`} onClick={() => addSource('Confluence')} aria-pressed={selectedSource === 'Confluence'} disabled={step2Locked}>
//                           <span className={styles.sourceBtnInner}><span className={styles.sourceBtnIcon}><Icon.Confluence size={big} /></span><span className={styles.sourceBtnLabel}>Confluence space</span></span>
//                         </button>
//                         <button type="button" className={`${styles.sourceBtn} ${selectedSource === 'File Upload' ? styles.sourceBtnActive : ''}`} onClick={() => addSource('File Upload')} aria-pressed={selectedSource === 'File Upload'} disabled={step2Locked}>
//                           <span className={styles.sourceBtnInner}><span className={styles.sourceBtnIcon}><Icon.File size={big} /></span><span className={styles.sourceBtnLabel}>File upload</span></span>
//                         </button>
//                         <button type="button" className={`${styles.sourceBtn} ${selectedSource === 'SharePoint' ? styles.sourceBtnActive : ''}`} onClick={() => addSource('SharePoint')} aria-pressed={selectedSource === 'SharePoint'} disabled>
//                           <span className={styles.sourceBtnInner}><span className={styles.sourceBtnIcon}><Icon.Globe size={big} /></span><span className={styles.sourceBtnLabel}>SharePoint</span></span>
//                         </button>
//                         <button type="button" className={`${styles.sourceBtn} ${selectedSource === 'OneDrive' ? styles.sourceBtnActive : ''}`} onClick={() => addSource('OneDrive')} aria-pressed={selectedSource === 'OneDrive'} disabled>
//                           <span className={styles.sourceBtnInner}><span className={styles.sourceBtnIcon}><Icon.Cloud size={big} /></span><span className={styles.sourceBtnLabel}>OneDrive</span></span>
//                         </button>
//                       </div>

//                       {selectedSource === 'Confluence' && (
//                         <section className={styles.sourcePanel} aria-labelledby="confluence-panel-title">
//                           <header className={styles.sourcePanelHeader}>
//                             <div className={styles.sourcePanelTitle} id="confluence-panel-title"><Icon.Confluence /><span>Confluence space</span></div>
//                             <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
//                               <button type="button" className={styles.btn} onClick={() => setSelectedSource('File Upload')} disabled={step2Locked}>Go to File upload</button>
//                               <button type="button" className={styles.sourcePanelClose} onClick={() => removeSource('Confluence')} aria-label="Remove Confluence source" disabled={step2Locked}>✕</button>
//                             </div>
//                           </header>
//                           <div className={styles.sourcePanelBody}>
//                             <div><label className={`${styles.label} ${styles.required}`} htmlFor="cf-name">Source name</label><input id="cf-name" className={styles.input} placeholder="Confluence space name" value={confluence.name} onChange={e => setConfluence(p => ({ ...p, name: e.target.value }))} disabled={step2Locked} /></div>
//                             <div><label className={`${styles.label} ${styles.required}`} htmlFor="cf-url">URL / path</label><input id="cf-url" className={styles.input} placeholder="https://company.atlassian.net/wiki/spaces/SPACE" value={confluence.url} onChange={e => setConfluence(p => ({ ...p, url: e.target.value }))} inputMode="url" autoComplete="url" disabled={step2Locked} /></div>
//                             <div><label className={styles.label} htmlFor="cf-desc">Description</label><textarea id="cf-desc" className={styles.textarea} placeholder="Brief description of this knowledge source" value={confluence.description} onChange={e => setConfluence(p => ({ ...p, description: e.target.value }))} disabled={step2Locked} /></div>

//                             <div className={styles.autorefresh}>
//                               <div className={styles.autorefreshRow}>
//                                 <label className={styles.autorefreshLabel}><input type="checkbox" checked={confluence.autoRefresh} onChange={e => setConfluence(p => ({ ...p, autoRefresh: e.target.checked }))} style={{ marginRight: 8 }} disabled={step2Locked} />Enable auto-sync</label>
//                                 <select className={styles.select} aria-label="Frequency" value={confluence.frequency} onChange={e => setConfluence(p => ({ ...p, frequency: e.target.value }))} disabled={step2Locked}><option>Daily</option><option>Weekly</option><option>Monthly</option></select>
//                                 <input className={styles.timeInput} type="time" aria-label="Preferred time" value={confluence.time} onChange={e => setConfluence(p => ({ ...p, time: e.target.value }))} disabled={step2Locked} />
//                               </div>
//                             </div>

//                             {/* Submit from Confluence panel */}
//                             <div style={{ display: 'flex', gap: 8, marginTop: 10 }}>
//                               <button type="button" className={styles.btn} onClick={submitStep2Sources} disabled={savingStep2 || step2Locked} style={{ background: 'var(--primary)', color: '#fff' }}>
//                                 {savingStep2 ? 'Submitting…' : (step2Saved ? 'Submitted ✓' : 'Submit')}
//                               </button>
//                             </div>
//                           </div>
//                         </section>
//                       )}

//                       {selectedSource === 'File Upload' && (
//                         <section className={styles.sourcePanel} aria-labelledby="file-panel-title">
//                           <header className={styles.sourcePanelHeader}>
//                             <div className={styles.sourcePanelTitle} id="file-panel-title"><Icon.File /><span>File upload</span></div>
//                             <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
//                               <button type="button" className={styles.btn} onClick={() => setSelectedSource('Confluence')} disabled={step2Locked}>Go to Confluence</button>
//                               <button type="button" className={styles.sourcePanelClose} onClick={() => removeSource('File Upload')} aria-label="Remove File Upload source" disabled={step2Locked}>✕</button>
//                             </div>
//                           </header>
//                           <div className={styles.sourcePanelBody}>
//                             <div><label className={`${styles.label} ${styles.required}`} htmlFor="fu-name">Source name</label><input id="fu-name" className={styles.input} placeholder="Source name" value={fileUpload.name} onChange={e => setFileUpload(p => ({ ...p, name: e.target.value }))} disabled={step2Locked} /></div>
//                             <div>
//                               <label className={styles.label} htmlFor="fu-files">Upload files</label>
//                               <div id="fu-files" style={{ ...dropzoneStyle, opacity: step2Locked ? 0.6 : 1, pointerEvents: step2Locked ? 'none' : 'auto' }} onDragOver={onDragOver} onDrop={onDrop} onClick={() => !step2Locked && fileInputRef.current?.click()} role="button" tabIndex={0} aria-label="Click to upload or drag and drop files">
//                                 <div style={{ display: 'grid', placeItems: 'center', gap: 8 }}>
//                                   <Icon.Upload size={24} />
//                                   <div>Click to upload or drag and drop</div>
//                                   <div style={{ fontSize: 12 }}>PDF, DOC, TXT, MD, JSON, XML files supported</div>
//                                 </div>
//                                 <input ref={fileInputRef} type="file" multiple accept=".pdf,.doc,.docx,.txt,.md,.json,.xml" style={{ display: 'none' }} onChange={onFilePick} />
//                               </div>
//                               {fileUpload.files.length > 0 && (
//                                 <ul style={{ margin: '8px 0 0', paddingLeft: 18 }}>
//                                   {fileUpload.files.map((f, idx) => (
//                                     <li key={`${f.name}-${idx}`} style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
//                                       <span>{f.name}</span>
//                                       {uploadStatuses[f.name] && (
//                                         <span style={{ fontSize: 12, color:
//                                           uploadStatuses[f.name] === 'done' ? '#166534' :
//                                           uploadStatuses[f.name] === 'uploading' ? '#1f2937' :
//                                           uploadStatuses[f.name] === 'error' ? '#9a3412' : '#6b7280'
//                                         }}>
//                                           {uploadStatuses[f.name] === 'done' && '✓ uploaded'}
//                                           {uploadStatuses[f.name] === 'uploading' && 'uploading…'}
//                                           {uploadStatuses[f.name] === 'error' && 'failed'}
//                                         </span>
//                                       )}
//                                       <button type="button" aria-label={`Remove ${f.name}`} onClick={() => removeFile(idx)} className={styles.sourcePanelClose} style={{ padding: '2px 6px' }} disabled={step2Locked}>✕</button>
//                                     </li>
//                                   ))}
//                                 </ul>
//                               )}
//                             </div>
//                             <div><label className={styles.label} htmlFor="fu-desc">Description</label><textarea id="fu-desc" className={styles.textarea} placeholder="Brief description of this knowledge source" value={fileUpload.description} onChange={e => setFileUpload(p => ({ ...p, description: e.target.value }))} disabled={step2Locked} /></div>
//                             <div className={styles.autorefresh}>
//                               <div className={styles.autorefreshRow}>
//                                 <label className={styles.autorefreshLabel}><input type="checkbox" checked={fileUpload.autoRefresh} onChange={e => setFileUpload(p => ({ ...p, autoRefresh: e.target.checked }))} style={{ marginRight: 8 }} disabled={step2Locked} />Enable auto-sync</label>
//                                 <select className={styles.select} aria-label="Frequency" value={fileUpload.frequency} onChange={e => setFileUpload(p => ({ ...p, frequency: e.target.value as any }))} disabled={step2Locked}><option>Daily</option><option>Weekly</option><option>Monthly</option></select>
//                                 <input className={styles.timeInput} type="time" aria-label="Preferred time" value={fileUpload.time} onChange={e => setFileUpload(p => ({ ...p, time: e.target.value }))} disabled={step2Locked} />
//                               </div>
//                             </div>

//                             {/* Submit from File Upload panel */}
//                             <div style={{ display: 'flex', gap: 8, marginTop: 10 }}>
//                               <button type="button" className={styles.btn} onClick={submitStep2Sources} disabled={savingStep2 || step2Locked} style={{ background: 'var(--primary)', color: '#fff' }}>
//                                 {savingStep2 ? 'Submitting…' : (step2Saved ? 'Submitted ✓' : 'Submit')}
//                               </button>
//                             </div>

//                             {saveErr2 && (
//                               <div role="alert" style={{ background: '#fff7ed', border: '1px solid #fed7aa', color: '#9a3412', padding: 10, borderRadius: 8, marginTop: 10 }}>
//                                 {saveErr2}
//                               </div>
//                             )}
//                           </div>
//                         </section>
//                       )}

//                       {selectedSource === null && (
//                         <div className={styles.emptyState} aria-live="polite">
//                           <div className={styles.emptyIcon}><Icon.Database /></div>
//                           <p className={styles.emptyText}>No knowledge sources added yet. Click the buttons above to get started.</p>
//                         </div>
//                       )}
//                     </>
//                   )}

//                   {/* Step 3 */}
//                   {activeStep === 3 && (
//                     <div style={{ display: 'grid', gap: 16 }}>
//                       <div style={{ background: '#eef2ff', color: '#1f2937', borderRadius: 10, padding: '14px 16px' }} aria-live="polite">
//                         <strong style={{ fontWeight: 600 }}>Sources to process: </strong>{sources.length > 0 ? sources.join(', ') : 'None selected'}
//                       </div>
//                       <div style={{ display: 'grid', placeItems: 'center', paddingTop: 4 }}>
//                         <button type="button" className={`${styles.btn} ${styles.btnPrimary}`} onClick={() => { if (processing) return; setProcessing(true); setProcessed(false); setTimeout(() => { setProcessing(false); setProcessed(true); }, 1600); }} disabled={processing} aria-busy={processing}>
//                           {processing ? 'Running…' : 'Start processing'}
//                         </button>
//                       </div>
//                     </div>
//                   )}

//                   {/* Step 4 */}
//                   {activeStep === 4 && (
//                     <div style={{ display: 'grid', gap: 18 }}>
//                       <div>
//                         <label className={styles.label}>Test your knowledge base</label>
//                         {testQuestions.map((q, i) => (
//                           <div key={i} style={{ display: 'flex', gap: 8, alignItems: 'center', marginBottom: 8 }}>
//                             <input className={styles.input} placeholder="Enter a test question..." value={q} onChange={e => { const next = [...testQuestions]; next[i] = e.target.value; setTestQuestions(next); }} />
//                             <button type="button" className={`${styles.btn} ${styles.btnGhost}`} onClick={() => { if (testing) return; setTesting(true); setTimeout(() => setTesting(false), 800); }} disabled={testing}>{testing ? 'Testing…' : 'Test question'}</button>
//                           </div>
//                         ))}
//                         <button type="button" className={`${styles.btn} ${styles.btnGhost}`} onClick={() => setTestQuestions(qs => [...qs, ''])} style={{ marginTop: 4 }}>+ Add test question</button>
//                       </div>
//                       <div role="status" aria-live="polite" style={{ background: '#e9fbe7', border: '1px solid '#bbf7d0', borderRadius: 8, padding: 14, color: '#14532d' }}>
//                         <div style={{ fontWeight: 600, marginBottom: 6 }}>Processing complete</div>
//                         <ul style={{ margin: 0, paddingLeft: 18, lineHeight: 1.6 }}>
//                           <li>{sources.length} knowledge {sources.length === 1 ? 'source' : 'sources'} processed</li>
//                           <li>{chunksCreated.toLocaleString()} content chunks created</li>
//                           <li>{qualityScore}% quality score</li>
//                           <li>Ready for deployment</li>
//                         </ul>
//                       </div>
//                       <div>
//                         <label className={styles.label}>Deployment options</label>
//                         <label style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}><input type="checkbox" checked={deployToCrewMate} onChange={e => setDeployToCrewMate(e.target.checked)} />Deploy to CrewMate</label>
//                         <div style={{ marginTop: 10 }}>
//                           <button type="button" className={styles.btn} style={{ background: deployed ? '#16a34a' : '#22c55e', color: '#fff', borderRadius: 8, padding: '12px 16px', fontWeight: 700 }} onClick={() => setDeployed(true)}>{deployed ? 'Deployed ✅' : 'Deploy knowledge base'}</button>
//                         </div>
//                       </div>
//                     </div>
//                   )}

//                   <div className={styles.formFooter}>
//                     <button
//                       type="button"
//                       className={`${styles.btn} ${styles.btnGhost}`}
//                       onClick={goPrev}
//                       disabled={
//                         activeStep === 1 ||
//                         processing ||
//                         (activeStep === 1 && step1Locked) ||
//                         (activeStep === 2 && step2Locked) // << grey out Prev on Step-2 once submitted
//                       }
//                     >
//                       Previous
//                     </button>
//                     <button
//                       type="button"
//                       className={`${styles.btn} ${styles.btnPrimary}`}
//                       onClick={() => { activeStep === 2 ? (step2Saved ? goNext() : submitStep2Sources()) : goNext(); }}
//                       disabled={!canContinue}
//                     >
//                       {activeStep === 3 && processing ? 'Processing…' : activeStep === TOTAL_STEPS ? 'Finish' : 'Continue'}
//                     </button>
//                   </div>
//                 </div>
//               </div>
//             </>
//           )}
//         </div>
//       </main>
//     </>
//   );
// };

// export default SelfServicePortal;

import React, { useEffect, useMemo, useRef, useState } from "react";
import styles from "./home.module.scss";
// ⚠️ If your utils path is different, adjust this import.
import { putS3, presignFiles, uploadWithPresigned } from "../../utils/s3";

/* =========================================================================
   Types & constants
   ========================================================================= */

type Section = "onboard" | "dashboard" | "knowledge" | "auto" | "analytics" | "settings";

type FormModel = {
  teamName: string;
  department: string;
  domain: string;
  contactEmail: string;
  description: string;
};

type ConfluenceModel = {
  sourceName: string;
  url: string;
  description: string;
  autoSync: boolean;
  frequency: "Daily" | "Weekly" | "Monthly";
  time: string; // "09:00"
};

type FilesModel = {
  sourceName: string;
  description: string;
  autoSync: boolean;
  frequency: "Daily" | "Weekly" | "Monthly";
  time: string;
  files: File[];
};

const DOMAINS = ["Select domain", "Support", "Engineering", "Sales", "HR", "Finance"];

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

/* =========================================================================
   Small shared UI bits (kept minimal to not disturb your styling)
   ========================================================================= */

const Pill: React.FC<{ tone?: "ok" | "warn" | "info"; children: React.ReactNode }> = ({
  tone = "info",
  children,
}) => {
  const toneClass =
    tone === "ok"
      ? styles.pillOk ?? "bg-green-100 text-green-800"
      : tone === "warn"
      ? styles.pillWarn ?? "bg-yellow-100 text-yellow-800"
      : styles.pillInfo ?? "bg-blue-100 text-blue-800";
  return <span className={`${styles.pill ?? ""} ${toneClass}`}>{children}</span>;
};

const Button: React.FC<
  React.ButtonHTMLAttributes<HTMLButtonElement> & { variant?: "primary" | "secondary" | "ghost" }
> = ({ variant = "primary", className = "", ...props }) => {
  const cls =
    variant === "secondary"
      ? styles.btnSecondary ?? "rounded-md border px-4 py-2"
      : variant === "ghost"
      ? styles.btnGhost ?? "px-3 py-2"
      : styles.btn ?? "rounded-md bg-blue-600 px-4 py-2 text-white";
  return <button {...props} className={`${cls} ${className}`} />;
};

/* =========================================================================
   Registration (Step 1)
   ========================================================================= */

const emptyForm: FormModel = {
  teamName: "",
  department: "",
  domain: "",
  contactEmail: "",
  description: "",
};

function validateRegistration(m: FormModel) {
  const errors: Partial<Record<keyof FormModel, string>> = {};
  if (!m.teamName.trim()) errors.teamName = "Team name is required";
  if (!m.department.trim()) errors.department = "Department is required";
  if (!m.domain || m.domain === "Select domain") errors.domain = "Please select a domain";
  if (!m.contactEmail.trim() || !EMAIL_RE.test(m.contactEmail)) errors.contactEmail = "Valid email required";
  // description optional
  return errors;
}

/* =========================================================================
   Knowledge Sources (Step 2)
   ========================================================================= */

const emptyConfluence: ConfluenceModel = {
  sourceName: "",
  url: "",
  description: "",
  autoSync: true,
  frequency: "Weekly",
  time: "09:00",
};

const emptyFiles: FilesModel = {
  sourceName: "",
  description: "",
  autoSync: true,
  frequency: "Weekly",
  time: "09:00",
  files: [],
};

function validateConfluence(m: ConfluenceModel) {
  const e: Partial<Record<keyof ConfluenceModel, string>> = {};
  if (!m.sourceName.trim()) e.sourceName = "Source name is required";
  if (!m.url.trim()) e.url = "URL is required";
  return e;
}

function validateFiles(m: FilesModel) {
  const e: Partial<Record<keyof FilesModel, string>> = {};
  if (!m.sourceName.trim()) e.sourceName = "Source name is required";
  // files optional: user may submit confluence-only or files-only
  return e;
}

/* =========================================================================
   Extra tabs (simple stubs so pages render)
   ========================================================================= */

const Dashboard: React.FC<{ indexName?: string; teamName?: string }> = ({ indexName, teamName }) => (
  <div className={styles.pageWrap}>
    <h2 className={styles.pageTitle}>Dashboard</h2>
    <div className={styles.cardRow}>
      <div className={styles.card}>
        <div className={styles.cardTitle}>Team</div>
        <div className={styles.cardBody}>{teamName || "—"}</div>
      </div>
      <div className={styles.card}>
        <div className={styles.cardTitle}>Pinecone index</div>
        <div className={styles.cardBody}>{indexName || "—"}</div>
      </div>
      <div className={styles.card}>
        <div className={styles.cardTitle}>Status</div>
        <div className={styles.cardBody}><Pill tone="ok">Healthy</Pill></div>
      </div>
    </div>
  </div>
);

const AutoRefresh: React.FC = () => (
  <div className={styles.pageWrap}>
    <h2 className={styles.pageTitle}>Auto-refresh (placeholder)</h2>
    <p className={styles.muted}>Wire this to your scheduler when ready.</p>
  </div>
);

const Analytics: React.FC = () => (
  <div className={styles.pageWrap}>
    <h2 className={styles.pageTitle}>Analytics (placeholder)</h2>
    <p className={styles.muted}>Add your charts / telemetry here.</p>
  </div>
);

const Settings: React.FC = () => (
  <div className={styles.pageWrap}>
    <h2 className={styles.pageTitle}>Settings (placeholder)</h2>
    <p className={styles.muted}>Non-blocking stub page.</p>
  </div>
);

/* =========================================================================
   Main component
   ========================================================================= */

const Home: React.FC = () => {
  const [section, setSection] = useState<Section>("onboard");

  // Step state
  const [reg, setReg] = useState<FormModel>({ ...emptyForm });
  const [regErrors, setRegErrors] = useState<Partial<Record<keyof FormModel, string>>>({});
  const [regSubmitting, setRegSubmitting] = useState(false);
  const [regDone, setRegDone] = useState(false);

  const [activeKnowledgePane, setActiveKnowledgePane] = useState<"confluence" | "files">("confluence");
  const [conf, setConf] = useState<ConfluenceModel>({ ...emptyConfluence });
  const [confErrors, setConfErrors] = useState<Partial<Record<keyof ConfluenceModel, string>>>({});
  const [filesModel, setFilesModel] = useState<FilesModel>({ ...emptyFiles });
  const [filesErrors, setFilesErrors] = useState<Partial<Record<keyof FilesModel, string>>>({});
  const [filesUploading, setFilesUploading] = useState(false);
  const [knowledgeSubmitting, setKnowledgeSubmitting] = useState(false);
  const [knowledgeDone, setKnowledgeDone] = useState(false);

  // Persist minimal flags so refreshes keep the lock state (not auth – just UX)
  useEffect(() => {
    try {
      const s = localStorage.getItem("coach.regDone");
      if (s === "1") setRegDone(true);
      const k = localStorage.getItem("coach.knowledgeDone");
      if (k === "1") setKnowledgeDone(true);
    } catch {}
  }, []);

  useEffect(() => {
    try {
      localStorage.setItem("coach.regDone", regDone ? "1" : "0");
      localStorage.setItem("coach.knowledgeDone", knowledgeDone ? "1" : "0");
    } catch {}
  }, [regDone, knowledgeDone]);

  /* ------------------------------- Actions ------------------------------ */

  const canContinueFromReg = regDone;
  const canContinueFromKnowledge = knowledgeDone;

  const onSubmitRegistration = async () => {
    const errors = validateRegistration(reg);
    setRegErrors(errors);
    if (Object.keys(errors).length) return;

    setRegSubmitting(true);
    try {
      // Store registration payload to S3 (key format can be anything your /api/put expects)
      await putS3({
        key: `teams/${reg.teamName || "team"}/registration.json`,
        body: JSON.stringify(reg),
        contentType: "application/json",
      });

      setRegDone(true);
    } catch (e) {
      console.error("Registration save failed:", e);
      alert("Registration failed. Please try again.");
    } finally {
      setRegSubmitting(false);
    }
  };

  const onSubmitKnowledge = async (from: "confluence" | "files") => {
    // validate the pane that is being submitted
    if (from === "confluence") {
      const errs = validateConfluence(conf);
      setConfErrors(errs);
      if (Object.keys(errs).length) return;
    } else {
      const errs = validateFiles(filesModel);
      setFilesErrors(errs);
      if (Object.keys(errs).length) return;
    }

    setKnowledgeSubmitting(true);
    try {
      // 1) Upload files (if any)
      if (from === "files" && filesModel.files && filesModel.files.length > 0) {
        setFilesUploading(true);
        const presigned = await presignFiles(
          filesModel.files.map((f) => ({ filename: f.name, contentType: f.type || "application/octet-stream" }))
        );
        await uploadWithPresigned(presigned, filesModel.files);
        setFilesUploading(false);
      }

      // 2) Save combined knowledge sources snapshot
      const payload = {
        confluence: conf,
        files: {
          sourceName: filesModel.sourceName,
          description: filesModel.description,
          autoSync: filesModel.autoSync,
          frequency: filesModel.frequency,
          time: filesModel.time,
          // (filenames only — actual file bytes are uploaded above)
          fileNames: (filesModel.files || []).map((f) => f.name),
        },
      };

      await putS3({
        key: `teams/${reg.teamName || "team"}/knowledge-sources.json`,
        body: JSON.stringify(payload),
        contentType: "application/json",
      });

      setKnowledgeDone(true);
    } catch (e) {
      console.error("Knowledge save failed:", e);
      alert("Saving knowledge sources failed. Please try again.");
    } finally {
      setKnowledgeSubmitting(false);
    }
  };

  /* ----------------------------- Render bits ---------------------------- */

  const renderTopNav = () => (
    <div className={styles.topnav}>
      <button className={section === "onboard" ? styles.tabActive : styles.tab} onClick={() => setSection("onboard")}>
        + Onboard team
      </button>
      <button className={section === "dashboard" ? styles.tabActive : styles.tab} onClick={() => setSection("dashboard")}>
        Dashboard
      </button>
      <button className={section === "knowledge" ? styles.tabActive : styles.tab} onClick={() => setSection("knowledge")}>
        Knowledge sources
      </button>
      <button className={section === "auto" ? styles.tabActive : styles.tab} onClick={() => setSection("auto")}>
        Auto-refresh
      </button>
      <button className={section === "analytics" ? styles.tabActive : styles.tab} onClick={() => setSection("analytics")}>
        Analytics
      </button>
      <button className={section === "settings" ? styles.tabActive : styles.tab} onClick={() => setSection("settings")}>
        Settings
      </button>
    </div>
  );

  const renderStepper = (active: number) => (
    <div className={styles.stepper}>
      {[1, 2, 3, 4].map((n) => (
        <span key={n} className={`${styles.step} ${n === active ? styles.stepActive : ""}`}>
          {n}
        </span>
      ))}
    </div>
  );

  /* --------------------------- Step 1: Register ------------------------- */

  const renderRegistration = () => (
    <div className={styles.pageWrap}>
      <div className={styles.headerRow}>
        <h2 className={styles.pageTitle}>Team registration</h2>
        <div className={styles.statusRight}>
          Step 1 of 4 <span className={styles.dotOnline}>●</span> System online
        </div>
      </div>

      {renderStepper(1)}

      <div className={`${styles.card} ${regDone ? styles.disabled : ""}`}>
        <div className={styles.formGrid}>
          <div className={styles.formItem}>
            <label className={styles.label}>Team name *</label>
            <input
              className={styles.input}
              value={reg.teamName}
              onChange={(e) => setReg((s) => ({ ...s, teamName: e.target.value }))}
              disabled={regDone}
              placeholder="e.g., Cloud Support Team"
            />
            {regErrors.teamName && <div className={styles.error}>{regErrors.teamName}</div>}
          </div>

          <div className={styles.formItem}>
            <label className={styles.label}>Department *</label>
            <input
              className={styles.input}
              value={reg.department}
              onChange={(e) => setReg((s) => ({ ...s, department: e.target.value }))}
              disabled={regDone}
              placeholder="e.g., ESAF"
            />
            {regErrors.department && <div className={styles.error}>{regErrors.department}</div>}
          </div>

          <div className={styles.formItem}>
            <label className={styles.label}>Domain *</label>
            <select
              className={styles.select}
              value={reg.domain || "Select domain"}
              onChange={(e) => setReg((s) => ({ ...s, domain: e.target.value }))}
              disabled={regDone}
            >
              {DOMAINS.map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </select>
            {regErrors.domain && <div className={styles.error}>{regErrors.domain}</div>}
          </div>

          <div className={styles.formItem}>
            <label className={styles.label}>Contact email *</label>
            <input
              className={styles.input}
              value={reg.contactEmail}
              onChange={(e) => setReg((s) => ({ ...s, contactEmail: e.target.value }))}
              disabled={regDone}
              placeholder="team-lead@company.com"
            />
            {regErrors.contactEmail && <div className={styles.error}>{regErrors.contactEmail}</div>}
          </div>

          <div className={styles.formItemFull}>
            <label className={styles.label}>Team description</label>
            <textarea
              className={styles.textarea}
              value={reg.description}
              onChange={(e) => setReg((s) => ({ ...s, description: e.target.value }))}
              disabled={regDone}
              placeholder="Brief description of your team's responsibilities"
              rows={4}
            />
          </div>
        </div>

        <div className={styles.actionsRow}>
          {/* No Previous button on page 1 */}
          <div className={styles.leftGroup}>
            {regDone ? (
              <Pill tone="ok">Team registration successful. You can continue.</Pill>
            ) : (
              <></>
            )}
          </div>

          <div className={styles.rightGroup}>
            {!regDone && (
              <Button onClick={onSubmitRegistration} disabled={regSubmitting}>
                {regSubmitting ? "Submitting..." : "Submit"}
              </Button>
            )}
            <Button
              variant="primary"
              disabled={!canContinueFromReg}
              onClick={() => setSection("knowledge")}
              className={styles.ml8}
            >
              Continue
            </Button>
          </div>
        </div>
      </div>
    </div>
  );

  /* ------------------------ Step 2: Knowledge sources ------------------- */

  const renderConfluencePanel = () => (
    <div className={`${styles.subCard} ${knowledgeDone ? styles.disabled : ""}`}>
      <div className={styles.subHeader}>
        <strong>Confluence space</strong>
        <div className={styles.linkRow}>
          <Button variant="ghost" onClick={() => setActiveKnowledgePane("files")}>
            Go to File upload
          </Button>
        </div>
      </div>

      <div className={styles.formGrid}>
        <div className={styles.formItem}>
          <label className={styles.label}>Source name *</label>
          <input
            className={styles.input}
            value={conf.sourceName}
            onChange={(e) => setConf((s) => ({ ...s, sourceName: e.target.value }))}
            disabled={knowledgeDone}
            placeholder="Confluence space name"
          />
          {confErrors.sourceName && <div className={styles.error}>{confErrors.sourceName}</div>}
        </div>

        <div className={styles.formItem}>
          <label className={styles.label}>URL / path *</label>
          <input
            className={styles.input}
            value={conf.url}
            onChange={(e) => setConf((s) => ({ ...s, url: e.target.value }))}
            disabled={knowledgeDone}
            placeholder="https://company.atlassian.net/wiki/spaces/SPACE"
          />
          {confErrors.url && <div className={styles.error}>{confErrors.url}</div>}
        </div>

        <div className={styles.formItemFull}>
          <label className={styles.label}>Description</label>
          <textarea
            className={styles.textarea}
            rows={3}
            value={conf.description}
            onChange={(e) => setConf((s) => ({ ...s, description: e.target.value }))}
            disabled={knowledgeDone}
            placeholder="Brief description of this knowledge source"
          />
        </div>

        <div className={styles.formItem}>
          <label className={styles.checkbox}>
            <input
              type="checkbox"
              checked={conf.autoSync}
              onChange={(e) => setConf((s) => ({ ...s, autoSync: e.target.checked }))}
              disabled={knowledgeDone}
            />
            <span>Enable auto-sync</span>
          </label>
        </div>

        <div className={styles.formItem}>
          <label className={styles.label}>Frequency</label>
          <select
            className={styles.select}
            value={conf.frequency}
            onChange={(e) => setConf((s) => ({ ...s, frequency: e.target.value as any }))}
            disabled={knowledgeDone}
          >
            <option>Daily</option>
            <option>Weekly</option>
            <option>Monthly</option>
          </select>
        </div>

        <div className={styles.formItem}>
          <label className={styles.label}>Time</label>
          <input
            type="time"
            className={styles.input}
            value={conf.time}
            onChange={(e) => setConf((s) => ({ ...s, time: e.target.value }))}
            disabled={knowledgeDone}
          />
        </div>
      </div>

      <div className={styles.actionsRow}>
        <div className={styles.leftGroup}>
          {knowledgeDone && <Pill tone="ok">Knowledge saved successfully. You can continue.</Pill>}
        </div>
        <div className={styles.rightGroup}>
          {!knowledgeDone && (
            <>
              <Button onClick={() => onSubmitKnowledge("confluence")} disabled={knowledgeSubmitting}>
                {knowledgeSubmitting ? "Submitting..." : "Submit"}
              </Button>
            </>
          )}
        </div>
      </div>
    </div>
  );

  const fileInputRef = useRef<HTMLInputElement | null>(null);

  const renderFilesPanel = () => (
    <div className={`${styles.subCard} ${knowledgeDone ? styles.disabled : ""}`}>
      <div className={styles.subHeader}>
        <strong>File upload</strong>
        <div className={styles.linkRow}>
          <Button variant="ghost" onClick={() => setActiveKnowledgePane("confluence")}>
            Go to Confluence
          </Button>
        </div>
      </div>

      <div className={styles.formGrid}>
        <div className={styles.formItem}>
          <label className={styles.label}>Source name *</label>
          <input
            className={styles.input}
            value={filesModel.sourceName}
            onChange={(e) => setFilesModel((s) => ({ ...s, sourceName: e.target.value }))}
            disabled={knowledgeDone}
            placeholder="Source name"
          />
          {filesErrors.sourceName && <div className={styles.error}>{filesErrors.sourceName}</div>}
        </div>

        <div className={styles.formItemFull}>
          <label className={styles.label}>Upload files</label>
          <div className={styles.dropArea} onClick={() => fileInputRef.current?.click()}>
            Click to upload or drag and drop
            <div className={styles.hint}>PDF, DOC, TXT, MD, JSON, XML files supported</div>
          </div>
          <input
            ref={fileInputRef}
            type="file"
            multiple
            className="hidden"
            disabled={knowledgeDone}
            onChange={(e) => {
              const fl = Array.from(e.target.files || []);
              setFilesModel((s) => ({ ...s, files: fl }));
            }}
          />
          {!!filesModel.files.length && (
            <ul className={styles.fileList}>
              {filesModel.files.map((f) => (
                <li key={f.name}>{f.name}</li>
              ))}
            </ul>
          )}
        </div>

        <div className={styles.formItemFull}>
          <label className={styles.label}>Description</label>
          <textarea
            className={styles.textarea}
            rows={3}
            value={filesModel.description}
            onChange={(e) => setFilesModel((s) => ({ ...s, description: e.target.value }))}
            disabled={knowledgeDone}
            placeholder="Brief description of this knowledge source"
          />
        </div>

        <div className={styles.formItem}>
          <label className={styles.checkbox}>
            <input
              type="checkbox"
              checked={filesModel.autoSync}
              onChange={(e) => setFilesModel((s) => ({ ...s, autoSync: e.target.checked }))}
              disabled={knowledgeDone}
            />
            <span>Enable auto-sync</span>
          </label>
        </div>

        <div className={styles.formItem}>
          <label className={styles.label}>Frequency</label>
          <select
            className={styles.select}
            value={filesModel.frequency}
            onChange={(e) => setFilesModel((s) => ({ ...s, frequency: e.target.value as any }))}
            disabled={knowledgeDone}
          >
            <option>Daily</option>
            <option>Weekly</option>
            <option>Monthly</option>
          </select>
        </div>

        <div className={styles.formItem}>
          <label className={styles.label}>Time</label>
          <input
            type="time"
            className={styles.input}
            value={filesModel.time}
            onChange={(e) => setFilesModel((s) => ({ ...s, time: e.target.value }))}
            disabled={knowledgeDone}
          />
        </div>
      </div>

      <div className={styles.actionsRow}>
        <div className={styles.leftGroup}>
          {filesUploading && <Pill>Uploading…</Pill>}
          {knowledgeDone && <Pill tone="ok">Knowledge saved successfully. You can continue.</Pill>}
        </div>
        <div className={styles.rightGroup}>
          {!knowledgeDone && (
            <Button onClick={() => onSubmitKnowledge("files")} disabled={knowledgeSubmitting || filesUploading}>
              {knowledgeSubmitting ? "Submitting..." : "Submit"}
            </Button>
          )}
        </div>
      </div>
    </div>
  );

  const renderKnowledge = () => (
    <div className={styles.pageWrap}>
      <div className={styles.headerRow}>
        <h2 className={styles.pageTitle}>Add your team’s knowledge sources</h2>
        <div className={styles.statusRight}>
          Step 2 of 4 <span className={styles.dotOnline}>●</span> System online
        </div>
      </div>

      {renderStepper(2)}

      {/* Picker boxes */}
      <div className={styles.sourceGrid}>
        <div
          className={`${styles.sourceCard} ${activeKnowledgePane === "confluence" ? styles.sourceActive : ""}`}
          onClick={() => setActiveKnowledgePane("confluence")}
        >
          <div className={styles.sourceIcon}>📄</div>
          <div className={styles.sourceTitle}>Confluence space</div>
        </div>
        <div
          className={`${styles.sourceCard} ${activeKnowledgePane === "files" ? styles.sourceActive : ""}`}
          onClick={() => setActiveKnowledgePane("files")}
        >
          <div className={styles.sourceIcon}>📁</div>
          <div className={styles.sourceTitle}>File upload</div>
        </div>
        <div className={styles.sourceCardDisabled}>
          <div className={styles.sourceIcon}>🌐</div>
          <div className={styles.sourceTitle}>SharePoint</div>
        </div>
        <div className={styles.sourceCardDisabled}>
          <div className={styles.sourceIcon}>☁️</div>
          <div className={styles.sourceTitle}>OneDrive</div>
        </div>
      </div>

      {/* Active config panel */}
      {activeKnowledgePane === "confluence" ? renderConfluencePanel() : renderFilesPanel()}

      <div className={styles.actionsRow}>
        {/* Prev is greyed out once submitted */}
        <Button
          variant="secondary"
          disabled={knowledgeDone}
          onClick={() => setSection("onboard")}
        >
          Previous
        </Button>

        <div className={styles.rightGroup}>
          <Button
            variant="primary"
            disabled={!canContinueFromKnowledge}
            onClick={() => setSection("dashboard" /* or "processing" if you have it */)}
          >
            Continue
          </Button>
        </div>
      </div>
    </div>
  );

  /* ------------------------------- Router ------------------------------- */

  const content = useMemo(() => {
    switch (section) {
      case "onboard":
        return renderRegistration();
      case "knowledge":
        return renderKnowledge();
      case "dashboard":
        return <Dashboard teamName={reg.teamName} indexName={`test-${reg.teamName || "team"}`} />;
      case "auto":
        return <AutoRefresh />;
      case "analytics":
        return <Analytics />;
      case "settings":
        return <Settings />;
      default:
        return null;
    }
  }, [section, reg, activeKnowledgePane, conf, filesModel, regDone, knowledgeDone, regErrors, confErrors, filesErrors, regSubmitting, knowledgeSubmitting, filesUploading]);

  return (
    <div className={styles.wrap}>
      {/* Top app header (kept minimal; uses your existing global styles) */}
      <div className={styles.appHeader}>
        <div>
          <div className={styles.appTitle}>COACH Self-Service Portal</div>
          <div className={styles.appSubtitle}>AI-powered knowledge management platform</div>
        </div>
        <div className={styles.headerRight}>
          <span className={styles.muted}>System online</span>
          <span className={styles.dotOnline}>●</span>
        </div>
      </div>

      {renderTopNav()}

      {content}
    </div>
  );
};

export default Home;
