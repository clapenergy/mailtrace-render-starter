import os, io, tempfile, traceback
from flask import Flask, render_template, request, redirect, url_for, send_file, flash, session, Markup
import pandas as pd

# Our pipeline + dashboard
from app.pipeline import run_pipeline
from app.dashboard_export import finalize_summary_for_export_v17, render_full_dashboard_v17
# NEW: schema guard
from app.schema_guard import analyze_dataframes

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # 200MB

@app.errorhandler(Exception)
def handle_exception(e):
    tb = traceback.format_exc()
    print("ERROR:", tb)
    return f"Something went wrong: {e}", 500

@app.route("/healthz")
def healthz():
    return "OK", 200

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

def _read_csv_file(fs) -> pd.DataFrame:
    # read safely as text
    return pd.read_csv(fs, dtype=str, keep_default_na=False)

def _apply_mapping(df: pd.DataFrame, mapping: dict, prefix: str) -> pd.DataFrame:
    """
    Rename df columns to canonical names using mapping entries that start with f"{prefix}:".
    """
    rename = {}
    for k, v in mapping.items():
        if not k.startswith(prefix + ":"):
            continue
        canon = k.split(":", 1)[1]  # e.g., "mail:address1" -> "address1"
        if v and v in df.columns:
            rename[v] = canon
    if rename:
        df = df.rename(columns=rename)
    return df

def _write_temp_csv(df: pd.DataFrame) -> str:
    f = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    df.to_csv(f.name, index=False)
    return f.name

@app.route("/run", methods=["POST"])
def run():
    mail_file = request.files.get("mail_csv")
    crm_file  = request.files.get("crm_csv")
    if not mail_file or not crm_file:
        flash("Please upload both CSV files.")
        return redirect(url_for("index"))

    # Read both into DataFrames for schema check
    try:
        mail_df = _read_csv_file(mail_file)
        crm_df  = _read_csv_file(crm_file)
    except Exception as e:
        flash(f"Could not read CSV files: {e}")
        return redirect(url_for("index"))

    # 1) Try schema guard (auto-detect or interactive resolve)
    mapping, mapping_html = analyze_dataframes(mail_df, crm_df)
    if mapping is None:
        # Stash current CSVs to temp files so /map can continue without re-upload
        mail_path = _write_temp_csv(mail_df)
        crm_path  = _write_temp_csv(crm_df)
        session["__mail_tmp"] = mail_path
        session["__crm_tmp"]  = crm_path
        # Show inline mapping page
        return mapping_html

    # 2) Mapping resolved -> rename columns to canonical so pipeline has what it needs
    mail_df_named = _apply_mapping(mail_df, mapping, "mail")
    crm_df_named  = _apply_mapping(crm_df,  mapping, "crm")

    # Write named CSVs to temp paths because the pipeline consumes file paths
    mail_path = _write_temp_csv(mail_df_named)
    crm_path  = _write_temp_csv(crm_df_named)

    # 3) Run your pipeline + finalize + render
    summary = run_pipeline(mail_path, crm_path)
    summary_v17 = finalize_summary_for_export_v17(summary)

    # Render dashboard
    mail_count_total = len(mail_df_named)
    # normalize notes "nan"/"NaN" -> "none"
    if "match_notes" in summary_v17.columns:
        summary_v17["match_notes"] = summary_v17["match_notes"].map(
            lambda x: (str(x).replace("NaN","none").replace("nan","none")) if isinstance(x, str) else x
        )

    html_out = render_full_dashboard_v17(summary_v17, mail_total_count=mail_count_total)

    # Persist export CSV in session for /download
    csv_bytes = summary_v17.to_csv(index=False).encode("utf-8")
    session["export_csv"] = csv_bytes.decode("utf-8")
    csv_len = len(csv_bytes)

    return render_template("result.html", dashboard_html=Markup(html_out), csv_len=csv_len)

@app.route("/map", methods=["POST"])
def map_columns():
    """
    Handles the interactive mapping form when schema guard couldn't auto-map.
    Expects the temp CSV paths stored in session by /run.
    """
    mail_tmp = session.get("__mail_tmp")
    crm_tmp  = session.get("__crm_tmp")
    if not (mail_tmp and crm_tmp and os.path.exists(mail_tmp) and os.path.exists(crm_tmp)):
        flash("Session expired. Please re-upload your files.")
        return redirect(url_for("index"))

    # Load back temp CSVs
    mail_df = pd.read_csv(mail_tmp, dtype=str, keep_default_na=False)
    crm_df  = pd.read_csv(crm_tmp,  dtype=str, keep_default_na=False)

    # Build mapping dict from form fields: keys like "mail:mail_date" -> selected header
    mapping = {}
    for k, v in request.form.items():
        if ":" in k and v.strip():
            mapping[k] = v.strip()

    # Apply mapping and continue like /run’s success path
    mail_df_named = _apply_mapping(mail_df, mapping, "mail")
    crm_df_named  = _apply_mapping(crm_df,  mapping, "crm")

    mail_path = _write_temp_csv(mail_df_named)
    crm_path  = _write_temp_csv(crm_df_named)

    try:
        summary = run_pipeline(mail_path, crm_path)
        summary_v17 = finalize_summary_for_export_v17(summary)
        mail_count_total = len(mail_df_named)
        if "match_notes" in summary_v17.columns:
            summary_v17["match_notes"] = summary_v17["match_notes"].map(
                lambda x: (str(x).replace("NaN","none").replace("nan","none")) if isinstance(x, str) else x
            )
        html_out = render_full_dashboard_v17(summary_v17, mail_total_count=mail_count_total)
        csv_bytes = summary_v17.to_csv(index=False).encode("utf-8")
        session["export_csv"] = csv_bytes.decode("utf-8")
        csv_len = len(csv_bytes)
        # Clean temp pointers
        session.pop("__mail_tmp", None)
        session.pop("__crm_tmp", None)
        return render_template("result.html", dashboard_html=html_out, csv_len=csv_len)
    except Exception as e:
        tb = traceback.format_exc()
        print("ERROR during /map -> pipeline:", tb)
        return f"Something went wrong: {e}", 500

@app.route("/download", methods=["POST"])
def download():
    csv_text = session.get("export_csv") or ""
    out = io.BytesIO(csv_text.encode("utf-8"))
    out.seek(0)
    return send_file(out, mimetype="text/csv", as_attachment=True, download_name="mailtrace_matches.csv")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
