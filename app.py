# app.py — MailTrace web app (Flask 3 compatible)
import os, io, tempfile, traceback
from flask import Flask, render_template, request, redirect, url_for, send_file, flash, session
from markupsafe import Markup  # <- Flask 3: Markup moved here
import pandas as pd

# Your pipeline & renderer
from app.pipeline import run_pipeline
from app.dashboard_export import finalize_summary_for_export_v17, render_full_dashboard_v17

app = Flask(__name__)

# Show full trace in logs + user-friendly message
@app.errorhandler(Exception)
def handle_exception(e):
    tb = traceback.format_exc()
    print("ERROR:", tb)   # shows up in Render logs
    return f"Something went wrong: {e}", 500

# Health check for Render
@app.route("/healthz")
def healthz():
    return "OK", 200

# Session + upload limits
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # 200MB

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/run", methods=["POST"])
def run():
    mail_file = request.files.get("mail_csv")
    crm_file  = request.files.get("crm_csv")
    if not mail_file or not crm_file:
        flash("Please upload both CSV files.")
        return redirect(url_for("index"))

    # Save uploads to temp files (pipeline expects file paths)
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as mf, \
         tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as cf:
        mf.write(mail_file.read())
        cf.write(crm_file.read())
        mf_path, cf_path = mf.name, cf.name

    # Run pipeline → summary
    summary = run_pipeline(mf_path, cf_path)

    # Normalize for export/render
    summary_v17 = finalize_summary_for_export_v17(summary)

    # Build dashboard HTML
    mail_count_total = len(pd.read_csv(mf_path, dtype=str, keep_default_na=False))
    # (optional: clean notes text)
    def _fix_notes(x):
        if not isinstance(x, str) or not x:
            return x
        return x.replace("NaN", "none").replace("nan", "none")
    if "Notes" in summary_v17.columns:
        summary_v17["Notes"] = summary_v17["Notes"].map(_fix_notes)

    html = render_full_dashboard_v17(summary_v17, mail_count_total)

    # Stash CSV in session for download
    csv_bytes = summary_v17.to_csv(index=False).encode("utf-8")
    session["export_csv"] = csv_bytes.decode("utf-8")
    csv_len = len(csv_bytes)

    # IMPORTANT: wrap with Markup so HTML isn’t escaped
    return render_template("result.html", dashboard_html=Markup(html), csv_len=csv_len)

@app.route("/download", methods=["POST"])
def download():
    csv_text = session.get("export_csv") or ""
    out = io.BytesIO(csv_text.encode("utf-8"))
    out.seek(0)
    return send_file(out, mimetype="text/csv", as_attachment=True, download_name="mailtrace_matches.csv")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
