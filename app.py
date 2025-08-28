import os, io, tempfile, uuid, traceback
from flask import Flask, render_template, request, redirect, url_for, send_file, flash, session
import pandas as pd

# Import your v17 glue + renderers
from app.pipeline import run_pipeline
from app.dashboard_export import finalize_summary_for_export_v17, render_full_dashboard_v17

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # 200MB uploads

# -------- Diagnostics --------
@app.errorhandler(Exception)
def handle_exception(e):
    tb = traceback.format_exc()
    print("ERROR:", tb)  # shows up in Render logs
    return f"Something went wrong: {e}", 500

@app.route("/healthz")
def healthz():
    return "OK", 200

# -------- Views --------
@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/run", methods=["POST"])
def run():
    try:
        mail_file = request.files.get("mail_csv")
        crm_file  = request.files.get("crm_csv")
        if not mail_file or not crm_file:
            flash("Please upload both CSV files.")
            return redirect(url_for("index"))

        # Save uploads to temp files (pipeline expects file paths)
        token = uuid.uuid4().hex
        mail_path = f"/tmp/mail_{token}.csv"
        crm_path  = f"/tmp/crm_{token}.csv"
        mail_file.save(mail_path)
        crm_file.save(crm_path)

        # Run pipeline (your v17 matcher is called inside)
        summary = run_pipeline(mail_path, crm_path)
        summary_v17 = finalize_summary_for_export_v17(summary)

        # Clean up note text just for display
        def _fix_notes(x):
            if not isinstance(x, str) or not x:
                return x
            return x.replace("NaN", "none").replace("nan", "none")
        if "match_notes" in summary_v17.columns:
            summary_v17["match_notes"] = summary_v17["match_notes"].map(_fix_notes)

        # Build dashboard HTML
        mail_count_total = len(pd.read_csv(mail_path, dtype=str, keep_default_na=False))
        html = render_full_dashboard_v17(summary_v17, mail_count_total)

        # Save export to /tmp; keep only a small token in session
        export_path = os.path.join(os.environ.get("TMPDIR", "/tmp"), f"mailtrace_export_{token}.csv")
        summary_v17.to_csv(export_path, index=False, encoding="utf-8")
        session["export_token"] = token

        return render_template("result.html", dashboard_html=html, csv_len=os.path.getsize(export_path))

    except Exception:
        tb = traceback.format_exc()
        print("ERROR in /run:", tb)
        flash("Something went wrong while processing your files. Check the logs.")
        return redirect(url_for("index"))

@app.route("/download", methods=["POST"])
def download():
    token = session.get("export_token")
    if not token:
        flash("No export available yet. Please run a match first.")
        return redirect(url_for("index"))

    export_path = os.path.join(os.environ.get("TMPDIR", "/tmp"), f"mailtrace_export_{token}.csv")
    if not os.path.exists(export_path):
        flash("Export expired. Please run the match again.")
        return redirect(url_for("index"))

    return send_file(export_path, mimetype="text/csv", as_attachment=True, download_name="mailtrace_matches.csv")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
