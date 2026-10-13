"""Run one .sql file against BigQuery and write EVERY result table to its own CSV.

Called by run_bq.ps1 -Mode csv (the VS Code task "BigQuery: Run current SQL file -> CSV").

usage: python run_bq_csv.py <sql_file> <out_dir>

Why Python instead of `bq --format=csv` (2026-10-13): for a script with several SELECT statements, `bq` prints
each statement's SQL text immediately before its result, with nothing separating the end of one result from
the next statement's echoed SQL, so the results cannot be split reliably (the old task kept only the LAST
result). `bq --format=json` separates them but sorts the columns alphabetically and drops the header of an
empty result. The BigQuery client reads each statement's result as its own job, with its schema, so every
result keeps its column order and its header, even with zero rows.

Output: <file>_<timestamp>.csv for a single result; <file>_<timestamp>__part1.csv, __part2.csv ... when the
script returns more than one. Nothing is written unless the query succeeds; any error exits non-zero.
"""
import csv, datetime, os, sys, warnings

warnings.filterwarnings("ignore", message=".*end user credentials.*")
from google.api_core import exceptions as gexc  # noqa: E402
from google.auth import exceptions as aexc  # noqa: E402
from google.cloud import bigquery  # noqa: E402

PROJECT = "xentity-sandbox-huy"


def results_of(client, job):
    """The SELECT results of a job, in statement order: the job itself, or its child jobs if it is a script."""
    if job.statement_type != "SCRIPT":
        return [job.result()]
    children = sorted(client.list_jobs(parent_job=job.job_id), key=lambda j: (j.created, j.job_id))
    return [c.result() for c in children if getattr(c, "statement_type", None) == "SELECT"]


def main(sql_file, out_dir):
    client = bigquery.Client(project=PROJECT)
    job = client.query(open(sql_file, encoding="utf-8").read())
    job.result()  # raises on any SQL or permission error
    results = results_of(client, job)
    if not results:
        print("The query ran but returned no result tables; no CSV written.")
        return 0

    os.makedirs(out_dir, exist_ok=True)
    base = os.path.splitext(os.path.basename(sql_file))[0] + "_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    names = [base + ".csv"] if len(results) == 1 else [f"{base}__part{k}.csv" for k in range(1, len(results) + 1)]
    written = []
    try:
        for name, rows in zip(names, results):  # write to .tmp first so a failure never leaves a partial CSV
            tmp = os.path.join(out_dir, name + ".tmp")
            written.append([tmp, os.path.join(out_dir, name), 0])
            with open(tmp, "w", newline="", encoding="utf-8") as fh:
                w = csv.writer(fh)
                w.writerow([f.name for f in rows.schema])
                for r in rows:
                    w.writerow(["" if v is None else v for v in r.values()])
                    written[-1][2] += 1
    except BaseException:
        for tmp, _, _ in written:
            if os.path.exists(tmp):
                os.remove(tmp)
        raise
    for tmp, final, n in written:
        os.replace(tmp, final)
        print(f"Wrote {final} ({n} data rows)")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    try:
        sys.exit(main(sys.argv[1], sys.argv[2]))
    except aexc.RefreshError as e:
        sys.exit(f"ERROR: Google sign-in expired ({e}).\nRun `gcloud auth application-default login` and retry.")
    except aexc.DefaultCredentialsError as e:
        sys.exit(f"ERROR: no Google credentials found ({e}).\nRun `gcloud auth application-default login` and retry.")
    except gexc.GoogleAPICallError as e:
        sys.exit(f"ERROR: BigQuery rejected the query. No CSV written.\n{e}")
