
import os, io, tempfile
from flask import Flask, render_template, request, redirect, url_for, send_file, flash, session
import pandas as pd

# Import your v17 logic
from app.pipeline import run_pipeline
from app.dashboard_export import finalize_summary_for_export_v17, render_full_dashboard_v17

app = Flask(__name__)
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

    # Save uploads to temp files because v17 pipeline expects file paths
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as mf, \
         tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as cf:
        mf.write(mail_file.read())
        cf.write(crm_file.read())
        mf_path, cf_path = mf.name, cf.name

    # Run your pipeline
    summary = run_pipeline(mf_path, cf_path)
    summary_v17 = finalize_summary_for_export_v17(summary)

    # Render dashboard HTML using your v17 renderer
    mail_count_total = len(pd.read_csv(mf_path, dtype=str))
    summary_v17_html = summary_v17.copy()
    # Normalize "nan" in notes if any
    def _fix_notes(x):
        if not isinstance(x, str) or not x:
            return x
        return x.replace("NaN", "none").replace("nan", "none")
    if "match_notes" in summary_v17_html.columns:
        summary_v17_html["match_notes"] = summary_v17_html["match_notes"].map(_fix_notes)

    html = render_full_dashboard_v17(summary_v17_html, mail_count_total)

    # Persist export CSV in session for /download
    csv_bytes = summary_v17.to_csv(index=False).encode("utf-8")
    session["export_csv"] = csv_bytes.decode("utf-8")  # store as text
    csv_len = len(csv_bytes)

    return render_template("result.html", dashboard_html=html, csv_len=csv_len)

@app.route("/download", methods=["POST"])
def download():
    csv_text = session.get("export_csv") or ""
    out = io.BytesIO(csv_text.encode("utf-8"))
    out.seek(0)
    return send_file(out, mimetype="text/csv", as_attachment=True, download_name="mailtrace_matches.csv")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
