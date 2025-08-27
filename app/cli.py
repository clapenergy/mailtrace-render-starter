import argparse, sys, pandas as pd, os
from .pipeline import run_pipeline
from .dashboard_export import finalize_summary_for_export_v17, render_full_dashboard_v17
def main():
    p = argparse.ArgumentParser(description="MailTrace end-to-end v17 (match + export)")
    p.add_argument("--mail", required=True, help="Path to raw mail CSV")
    p.add_argument("--crm", required=True, help="Path to raw CRM CSV")
    p.add_argument("--out-base", required=True, help="Output base path (without extension)")
    args = p.parse_args()
    try:
        summary = run_pipeline(args.mail, args.crm)
    except Exception as e:
        print(f"[error] matching pipeline failed: {e}", file=sys.stderr); sys.exit(2)
    try:
        summary_short = finalize_summary_for_export_v17(summary)
    except Exception as e:
        print(f"[error] finalize/export failed: {e}", file=sys.stderr); sys.exit(3)
    csv_out = args.out_base + ".csv"
    html_out = args.out_base + ".html"
    summary_short.to_csv(csv_out, index=False)
    try:
        mail_df = pd.read_csv(args.mail, dtype=str); total_mail = len(mail_df)
    except Exception:
        total_mail = len(summary_short)
    html = render_full_dashboard_v17(summary_short, mail_count_total=total_mail)
    with open(html_out, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[ok] wrote {csv_out}"); print(f"[ok] wrote {html_out}")
if __name__ == "__main__":
    main()
