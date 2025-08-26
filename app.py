import os
import io
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, send_file, flash
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")  # replace in prod
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # 200MB

ALLOWED_EXTENSIONS = {"csv"}

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

def normalize_address(df):
    # Very light placeholder normalization. Replace with your real logic.
    def norm(x):
        if pd.isna(x):
            return ""
        return (
            str(x)
            .strip()
            .lower()
            .replace(".", "")
            .replace(",", "")
            .replace(" street", " st")
            .replace(" avenue", " ave")
        )
    for col_guess in ["address", "address1", "address_1", "street", "Street", "Address", "Address1"]:
        if col_guess in df.columns:
            df["__addr_norm__"] = df[col_guess].map(norm)
            return df
    # If not found, try to concatenate common pieces
    for a,b in [("address1","address2"), ("street","unit")]:
        if a in df.columns:
            df["__addr_norm__"] = (df[a].fillna("") + " " + df.get(b, pd.Series([""] * len(df))).fillna("")).map(norm)
            return df
    # fallback: use first column
    df["__addr_norm__"] = df.iloc[:,0].map(norm)
    return df

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/upload", methods=["POST"])
def upload():
    mail_file = request.files.get("mail_csv")
    crm_file = request.files.get("crm_csv")

    if not mail_file or not crm_file:
        flash("Please upload both CSV files.")
        return redirect(url_for("index"))

    for f in (mail_file, crm_file):
        if not allowed_file(f.filename):
            flash("Only .csv files are allowed.")
            return redirect(url_for("index"))

    # Read CSVs into DataFrames
    try:
        mail_df = pd.read_csv(mail_file)
    except Exception:
        mail_file.stream.seek(0)
        mail_df = pd.read_csv(mail_file, encoding_errors="ignore")

    try:
        crm_df = pd.read_csv(crm_file)
    except Exception:
        crm_file.stream.seek(0)
        crm_df = pd.read_csv(crm_file, encoding_errors="ignore")

    # Shallow normalization + naive match on normalized address
    mail_df = normalize_address(mail_df)
    crm_df = normalize_address(crm_df)

    merged = pd.merge(
        mail_df,
        crm_df,
        on="__addr_norm__",
        how="inner",
        suffixes=("_mail", "_crm")
    )

    # KPIs (simple placeholders)
    kpis = {
        "mail_rows": int(len(mail_df)),
        "crm_rows": int(len(crm_df)),
        "matches": int(len(merged)),
        "match_rate_vs_mail_%": round(100.0 * (len(merged) / max(1, len(mail_df))), 2),
        "match_rate_vs_crm_%": round(100.0 * (len(merged) / max(1, len(crm_df))), 2),
    }

    # Limit preview table size for rendering
    preview_rows = merged.head(200)

    # Also offer a CSV download of all matches
    csv_buf = io.StringIO()
    merged.drop(columns=["__addr_norm__"], errors="ignore").to_csv(csv_buf, index=False)
    csv_data = csv_buf.getvalue().encode("utf-8")

    return render_template(
        "result.html",
        kpis=kpis,
        columns=list(preview_rows.columns.drop("__addr_norm__", errors="ignore")),
        rows=preview_rows.drop(columns=["__addr_norm__"], errors="ignore").values.tolist(),
        download_bytes=len(csv_data)
    )

@app.route("/download", methods=["POST"])
def download():
    # Re-run parse & merge to create a downloadable file from the uploaded content
    mail_file = request.files.get("mail_csv")
    crm_file = request.files.get("crm_csv")

    # re-parse (in a real app you'd persist to S3 + reference key)
    mail_df = pd.read_csv(mail_file)
    crm_df = pd.read_csv(crm_file)
    mail_df = normalize_address(mail_df)
    crm_df = normalize_address(crm_df)
    merged = pd.merge(mail_df, crm_df, on="__addr_norm__", how="inner", suffixes=("_mail", "_crm"))
    out = io.BytesIO()
    merged.drop(columns=["__addr_norm__"], errors="ignore").to_csv(out, index=False)
    out.seek(0)
    return send_file(out, mimetype="text/csv", as_attachment=True, download_name="mailtrace_matches.csv")

if __name__ == "__main__":
    # Local dev
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
