# MailTrace — Full Beta Bundle

Features:
- Upload 2 CSVs (Mail History + CRM)
- Auto-detect address columns → confirm mapping UI
- Address normalization + unit parsing
- Confidence scoring (unit mismatch penalty)
- KPI cards + preview table
- "none" rule shown only in `match_notes`; blanks elsewhere
- Optional Basic Auth: set `MAILTRACE_PASSWORD` env var
- Render-ready (Procfile + requirements)

## Deploy on Render

1) Connect repo → **Web Service**
2) Build Command: `pip install -r requirements.txt`
3) Start Command: `gunicorn app:app`
4) Optional env vars:
   - `SECRET_KEY` = long random string
   - `MAILTRACE_PASSWORD` = (if you want a password gate)

## Notes
- Files are processed in memory; no persistence. For production, wire S3 and pass object keys between routes.
