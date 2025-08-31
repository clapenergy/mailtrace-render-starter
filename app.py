import os, io, tempfile, traceback, base64
from flask import Flask, render_template, request, redirect, url_for, send_file, flash, session
import pandas as pd

# IMPORTANT: Markup is in markupsafe (not flask)
from markupsafe import Markup

# Your matcher + dashboard renderer
from mailtrace_matcher import run_matching
from dashboard_export import render_full_dashboard

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # 200MB uploads


# ---- health ----
@app.route("/healthz")
def healthz():
    return "OK", 200


# ---- pages ----
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

        # Save uploads to temp files for consistent pandas reads
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as mf, \
             tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as cf:
            mf.write(mail_file.read())
            cf.write(crm_file.read())
            mail_path, crm_path = mf.name, cf.name

        # Read raw for counts + pass to matcher
        mail_raw = pd.read_csv(mail_path, dtype=str, keep_default_na=False)
        crm_raw  = pd.read_csv(crm_path,  dtype=str, keep_default_na=False)

        # Run matching (returns normalized summary rows, including mail_dates list and parsed amounts)
        summary_df = run_matching(mail_raw, crm_raw)

        # Build full dashboard HTML (KPIs, top lists, chart, summary table)
        html = render_full_dashboard(
            summary_df=summary_df,
            mail_all_df=mail_raw,
            crm_all_df=crm_raw,
            brand_logo_url=url_for("static", filename="logo.png")
        )

        # Persist export CSV for download
        csv_bytes = summary_df.to_csv(index=False).encode("utf-8")
        session["export_csv"] = csv_bytes.decode("utf-8")
        csv_len = len(csv_bytes)

        return render_template("result.html", dashboard_html=Markup(html), csv_len=csv_len)
    except Exception as e:
        tb = traceback.format_exc()
        print("ERROR in /run:", tb)
        flash(f"Something went wrong while processing your files. ({e})")
        return redirect(url_for("index"))


@app.route("/download", methods=["POST"])
def download():
    csv_text = session.get("export_csv") or ""
    out = io.BytesIO(csv_text.encode("utf-8"))
    out.seek(0)
    return send_file(out, mimetype="text/csv", as_attachment=True, download_name="mailtrace_matches.csv")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
