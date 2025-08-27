MailTrace — Fresh Site (Drop-In)

What this is:
- Complete Flask app ready for Render
- Your logo wired everywhere (header) and as favicons
- Pages: upload → confirm mapping → results → download

Deploy / Replace:
1) On GitHub, open your repo (main branch). Press '.' to open the web editor.
2) Delete everything in the repo (Explorer left panel → right-click → Delete).
3) Click 'Upload' (or drag the contents of this zip into the editor). Commit.
4) Render auto-redeploys. Visit your URL.

Render settings (already compatible):
- Build: pip install -r requirements.txt
- Start: gunicorn app:app

Optional env vars:
- SECRET_KEY = any long random string
- MAILTRACE_PASSWORD = if you want a password gate on /