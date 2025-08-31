import os, io, tempfile, traceback
from flask import Flask, render_template, request, redirect, url_for, send_file, flash, session
import pandas as pd

# Import matching + dashboard logic
from app.mailtrace_matcher import run_matching
from app.dashboard_export import finalize_summary_for_export_v17, render_full_dashboard_v17

app = Flask(__name__)

# Error handler to log tracebacks
@app.errorhandler(Exception)
def handle_exception(e):
    tb = traceback.format_exc()
    print("ERROR:", tb)
    return f"Something went wrong: {e}", 500

app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # 200MB

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/run", methods=["POST"])
def run():
    mail_file = request.files.get("mail_csv")
    crm_file = request.files.get("crm_csv")
    if not mail_file or not crm_file:
        flash("Please upload both CSV files.")
        return redirect(url_for("index"))

    # Save uploads to temp files
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as mf, \
         tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as cf:
        mf.write(mail_file.read())
        cf.write(crm_file.read())
        mf_path, cf_path = mf.name, cf.name

    # Run matcher
    summary = run_matching(pd.read_csv(mf_path, dtype=str), pd.read_csv(cf_path, dtype=str))
    summary_v17 = finalize_summary_for_export_v17(summary)

    # Fix NaNs in notes
    def _fix_notes(x):
        if not isinstance(x, str) or not x:
            return x
        return x.replace("NaN", "none").replace("nan", "none")
    if "match_notes" in summary_v17.columns:
        summary_v17["match_notes"] = summary_v17["match_notes"].map(_fix_notes)

    # Render dashboard
    mail_count_total = len(pd.read_csv(mf_path, dtype=str))
    html = render_full_dashboard_v17(summary_v17, mail_count_total)

    # Save CSV for download
    csv_bytes = summary_v17.to_csv(index=False).encode("utf-8")
    session["export_csv"] = csv_bytes.decode("utf-8")

    return render_template("result.html", dashboard_html=html, csv_len=len(csv_bytes))

@app.route("/download", methods=["POST"])
def download():
    csv_text = session.get("export_csv") or ""
    out = io.BytesIO(csv_text.encode("utf-8"))
    out.seek(0)
    return send_file(out, mimetype="text/csv", as_attachment=True, download_name="mailtrace_matches.csv")

@app.route("/healthz")
def healthz():
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
