"""Build and publish the tables behind the DQIMP Web Experience.

    python <path to this file> build   [--page mlrs_case_completeness]
    python <path to this file> publish [--page mlrs_case_completeness]
    python <path to this file> display [--page mlrs_case_completeness]

build    Runs a page's SQL in one BigQuery session and writes one CSV per output table to
         the output folder (outside the repo -- the repo is public and the case table holds
         serial numbers). Nothing is written back to BigQuery.
publish  Overwrites each hosted table in the BLM HQ Portal from its CSV, keeping the item
         ID, so every dashboard and Web Experience widget built on it updates in place.
display  Sets each hosted table's title, layer name and column display names. No upload, no
         data change; publish runs the same step itself.

Run in the arcpro_clone environment (ArcGIS Pro's Python plus the google-cloud-bigquery
packages -- see README.md next to this file for the install). Settings, including the snapshot
list, live in webapp_config.json next to this file.

Folders are found from this file's own location, so it runs from any working directory and in
either layout:
  repo   <repo>/web-app/tests/dqimp_webapp.py   outputs -> output_dir in the config, relative to
                                                 the repo root (../Query_Results/web-app)
  flat   <folder>/tests/dqimp_webapp.py         outputs -> <folder>/Query_Results, used whenever
                                                 a Query_Results folder sits next to this file's folder
The page's SQL is always in a subfolder next to this file (sql_dir in the config).
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]   # only meaningful in the repo layout
CONFIG_PATH = SCRIPT_DIR / "webapp_config.json"
SNAPSHOT_LINE = re.compile(r"^DECLARE snapshot_dates .*-- @@SNAPSHOTS@@$", re.MULTILINE)


def load_config():
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


def resolve(path_str):
    p = Path(path_str)
    return p if p.is_absolute() else (REPO_ROOT / p).resolve()


def read_sql(page_cfg, name):
    """The page's SQL lives in a folder next to this script (sql_dir in the config)."""
    sql_dir = Path(page_cfg["sql_dir"])
    if not sql_dir.is_absolute():
        sql_dir = SCRIPT_DIR / sql_dir
    path = sql_dir / name
    if not path.exists():
        sys.exit(f"SQL file not found: {path}")
    return path.read_text(encoding="utf-8")


def output_dir(cfg, page):
    """Where the CSVs go: --output-dir if given; else a Query_Results folder next to this
    script's folder (the flat layout of a deployed copy); else output_dir in the config,
    relative to the repo root (the repo layout)."""
    if cfg.get("_output_dir_override"):
        base = Path(cfg["_output_dir_override"]).resolve()
    elif (SCRIPT_DIR.parent / "Query_Results").is_dir():
        base = SCRIPT_DIR.parent / "Query_Results"
    else:
        base = resolve(cfg["output_dir"])
    return base / page


def with_snapshots(sql, snapshots):
    """Replace the DECLARE line marked @@SNAPSHOTS@@ with the configured list."""
    if not all(re.fullmatch(r"\d{8}", s) for s in snapshots):
        sys.exit(f"Snapshots must be YYYYMMDD strings: {snapshots}")
    if snapshots != sorted(snapshots):
        sys.exit("Snapshots must be in ascending order -- the flows compare by position.")
    literal = ",".join(f"'{s}'" for s in snapshots)
    new_line = f"DECLARE snapshot_dates ARRAY<STRING> DEFAULT [{literal}];  -- @@SNAPSHOTS@@"
    sql, n = SNAPSHOT_LINE.subn(new_line, sql)
    if n != 1:
        sys.exit("Build SQL must contain exactly one '-- @@SNAPSHOTS@@' DECLARE line.")
    return sql


def trust_windows_certificates():
    """Let the BigQuery client trust what Windows trusts.

    Some networks inspect TLS: HTTPS to Google arrives re-signed by the network's certificate, which
    Windows trusts but Python's own certificate list (certifi) does not, so BigQuery calls fail
    with CERTIFICATE_VERIFY_FAILED (found 2026-10-09). This writes one bundle -- certifi's list
    plus the Windows root and intermediate stores -- and points requests at it. Verification
    stays ON; nothing is trusted that Windows does not already trust. Read fresh on every run,
    so a renewed network certificate needs no action. Skipped if REQUESTS_CA_BUNDLE is already set,
    or off Windows.
    """
    import os
    import ssl
    import tempfile

    if os.environ.get("REQUESTS_CA_BUNDLE") or sys.platform != "win32":
        return
    import certifi

    server_auth = "1.3.6.1.5.5.7.3.1"
    pems = [Path(certifi.where()).read_text(encoding="utf-8")]
    for store in ("ROOT", "CA"):
        for der, encoding, trust in ssl.enum_certificates(store):
            if encoding == "x509_asn" and (trust is True or server_auth in trust):
                pems.append(ssl.DER_cert_to_PEM_cert(der))
    bundle = Path(tempfile.gettempdir()) / "dqimp_webapp_ca_bundle.pem"
    bundle.write_text("\n".join(pems), encoding="utf-8")
    os.environ["REQUESTS_CA_BUNDLE"] = str(bundle)
    print(f"[build] trusting the Windows certificate store ({len(pems) - 1} certificates added to certifi's list)")


def build(cfg, page):
    import warnings

    if cfg.get("trust_windows_cert_store", True):
        trust_windows_certificates()
    # Signing in with personal gcloud credentials always warns about a missing "quota project".
    # Harmless here: every query names its project explicitly (bigquery_project in the config).
    warnings.filterwarnings("ignore", message="Your application has authenticated using end user credentials")
    from google.cloud import bigquery

    page_cfg = cfg["pages"][page]
    snapshots = page_cfg["snapshots"]
    out_dir = output_dir(cfg, page)
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[build] output folder: {out_dir}")

    client = bigquery.Client(project=cfg["bigquery_project"])
    started = time.time()
    print(f"[build] {page}: {len(snapshots)} snapshots {snapshots[0]} -> {snapshots[-1]}")

    # Step 1 creates the session; its temp tables live only inside that session.
    job = client.query(
        with_snapshots(read_sql(page_cfg, page_cfg["build_sql"]), snapshots),
        job_config=bigquery.QueryJobConfig(create_session=True),
    )
    canary = list(job.result())
    session_id = job.session_info.session_id
    in_session = bigquery.QueryJobConfig(
        connection_properties=[bigquery.ConnectionProperty("session_id", session_id)]
    )
    built = {r["Snapshot"]: r["Cases_In_Scope"] for r in canary}
    missing = [s for s in snapshots if not built.get(s)]
    if missing:
        sys.exit(f"[build] No in-scope cases for {missing} -- is the snapshot loaded?")
    for s in snapshots:
        print(f"  {s}: {built[s]:,} in-scope cases")

    manifest = {
        "page": page,
        "snapshots": snapshots,
        "built_at": datetime.now().isoformat(timespec="seconds"),
        "bigquery_project": cfg["bigquery_project"],
        "outputs": {},
    }
    try:
        for name, out in page_cfg["outputs"].items():
            df = client.query(read_sql(page_cfg, out["sql"]), job_config=in_session).to_dataframe()
            # The client reads checks by name; internal test IDs never go into a published table.
            if "Test_ID" in df.columns:
                sys.exit(f"[build] {name} has a Test_ID column -- test IDs must not be published.")
            if name == "flows" and "Balance_Check" in df:
                bad = df[df["Balance_Check"].fillna(0) != 0]
                if len(bad):
                    sys.exit(f"[build] Flows do not balance on {len(bad)} rows:\n{bad}")
            path = out_dir / out["csv"]
            df.to_csv(path, index=False, encoding="utf-8")
            manifest["outputs"][name] = {"csv": str(path), "rows": len(df), "columns": list(df.columns)}
            print(f"  {name}: {len(df):,} rows -> {path}")
    finally:
        client.query("CALL BQ.ABORT_SESSION()", job_config=in_session).result()

    manifest["seconds"] = round(time.time() - started)
    (out_dir / "build_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"[build] done in {manifest['seconds']}s")


def connect_portal(cfg):
    """Sign in to the BLM HQ Portal. 'pro' reuses ArcGIS Pro's own sign-in, exactly as it
    is in Pro, and nothing is stored here."""
    import warnings
    from arcgis.gis import GIS

    # arcgis 2.3+ flags content.add as deprecated (removed in 3.0); it still works in 2.x.
    warnings.filterwarnings("ignore", message=r".*is deprecated as of 2\.3\.0.*")

    portal = cfg.get("portal", {})
    auth = portal.get("auth", "pro")
    if auth == "pro":
        gis = GIS("pro")
    elif auth == "oauth":
        gis = GIS(portal["url"], client_id=portal["client_id"])   # opens a browser to sign in
    else:
        sys.exit(f"Unknown portal auth '{auth}'. Use 'pro' or 'oauth'.")
    print(f"[publish] signed in to {gis.url} as {gis.users.me.username}")
    harden_uploads(gis)
    return gis


UPLOAD_PART_BYTES = 8 * 1024 * 1024   # the Portal's smallest allowed part is 5 MB
UPLOAD_ATTEMPTS = 5


def harden_uploads(gis):
    """Upload large CSVs in small parts, one at a time, retrying a dropped part.

    The arcgis library sends a large file as 35 MB parts, up to five at once. On the HQ Portal's network
    the 106 MB Cases CSV failed that way (2026-10-19: SSLEOFError, 'EOF occurred in violation
    of protocol' on addPart): the connection was dropped mid-upload. That is a dropped
    connection, not a certificate problem, so certificate checking stays ON; the library's hint
    to set verify_cert=False is not followed. Covers both paths that upload a file: creating
    the source CSV item and overwriting it later (manager.overwrite -> Item.update), because
    both go through ContentManager._add_by_part -> connection.post_multipart.
    """
    import threading
    import requests
    from arcgis.gis import ContentManager

    if hasattr(ContentManager, "_calculate_upload_size"):
        ContentManager._calculate_upload_size = lambda self, fp: UPLOAD_PART_BYTES
    else:
        print("    note: this arcgis release sizes upload parts itself; parts are still sent one at a time")

    con = gis._con
    send_part = con.post_multipart
    one_at_a_time = threading.Lock()

    def post_multipart(*args, **kwargs):
        part = (kwargs.get("params") or {}).get("partNum", "?")
        with one_at_a_time:
            for attempt in range(1, UPLOAD_ATTEMPTS + 1):
                for f in kwargs.get("files") or []:   # rewind the part's data before each try
                    stream = f[1] if isinstance(f, (tuple, list)) and len(f) > 1 else None
                    if hasattr(stream, "seek"):
                        stream.seek(0)
                try:
                    result = send_part(*args, **kwargs)
                    if part != "?":
                        print(f"    part {part} uploaded")
                    return result
                except requests.exceptions.RequestException as e:
                    if attempt == UPLOAD_ATTEMPTS:
                        raise
                    wait = 15 * attempt
                    print(f"    part {part}: connection dropped ({type(e).__name__}); "
                          f"retry {attempt} of {UPLOAD_ATTEMPTS - 1} in {wait}s")
                    time.sleep(wait)

    con.post_multipart = post_multipart


def csv_row_count(csv_path):
    """Data rows in a CSV (header excluded), read the way a CSV reader reads it, so a quoted
    value containing a line break still counts as one row."""
    import csv
    with open(csv_path, newline="", encoding="utf-8") as f:
        return sum(1 for _ in csv.reader(f)) - 1


def save_config(cfg):
    CONFIG_PATH.write_text(json.dumps(cfg, indent=2) + "\n", encoding="utf-8")


# Columns that look numeric but must stay text. The Portal infers CSV column types, and would
# read '007500' as the number 7500 -- the same leading-zero trap as the taxonomy join (S4).
TEXT_FIELDS = {"BLM_Product_Code", "Snapshot", "Case_ID", "Serial_Number"}
TEXT_FIELDS_LOWER = {n.lower() for n in TEXT_FIELDS}
# Column names are compared case-insensitively everywhere: the HQ Portal (Enterprise 11.4) stores
# hosted-table field names in lower case (Subject_Name -> subject_name; found 2026-10-26), while
# the config and CSV headers use mixed case.


def service_name(title):
    """'MLRS Case Completeness - Cases' -> 'MLRS_Case_Completeness_Cases'."""
    return re.sub(r"_+", "_", re.sub(r"[^A-Za-z0-9]", "_", title)).strip("_")


def apply_display_settings(gis, item_id, out, aliases):
    """Make the hosted table readable in the Portal and in Dashboards, without touching its data:
      * item title    -> out["title"]       (publishing from a CSV item copies the CSV item's
                                              title, "... (source CSV)", onto the hosted table)
      * layer name    -> out["layer_name"]  (e.g. "Cases")
      * column alias  -> field_aliases      (e.g. Case_Type_Group -> "Case type group")
    Re-reads the item first and always reports what it found, so it can never do nothing
    silently. Run after create, after every overwrite (an overwrite can reset names and
    aliases), or on its own with the 'display' command. The item ID never changes.
    """
    item = gis.content.get(item_id)
    if item is None:
        print(f"    WARNING: item {item_id} not found -- display settings not applied.")
        return
    title = out.get("title")
    if title and item.title != title:
        old_title = item.title
        try:
            item.update(item_properties={"title": title})
            print(f"    item title: '{old_title}' -> '{title}'")
        except Exception as e:
            print(f"    WARNING: could not rename the item ({e}).")
    else:
        print(f"    item title: '{item.title}' (already right)")

    tables = (item.tables or []) + (item.layers or [])
    if not tables:
        print(f"    WARNING: item {item_id} has no table or layer yet -- names and aliases not applied. "
              "Run the 'display' command again in a minute.")
        return
    by_lower = {k.lower(): v for k, v in aliases.items()}
    for t in tables:
        fields = [dict(f) for f in t.properties.fields]
        in_config = [f for f in fields if f.get("name", "").lower() in by_lower]
        to_change = [{"name": f["name"], "alias": by_lower[f["name"].lower()]}
                     for f in in_config if f.get("alias") != by_lower[f["name"].lower()]]
        definition = {}
        layer_name = out.get("layer_name")
        if layer_name and t.properties.name != layer_name:
            definition["name"] = layer_name
        if to_change:
            definition["fields"] = to_change
        not_matched = [f["name"] for f in fields
                       if f.get("name", "").lower() not in by_lower and f.get("name", "").lower() not in ("objectid", "fid")]
        # Report the types that matter: code columns must be text (leading zeros), and list the
        # date columns (they decide what Dashboards offers as a date category).
        types = {f["name"].lower(): f.get("type", "").replace("esriFieldType", "") for f in fields}
        text_check = {n: types.get(n, "missing") for n in sorted(TEXT_FIELDS_LOWER) if n in types}
        bad = [n for n, ty in text_check.items() if ty != "String"]
        print(f"    column types: text columns {text_check}"
              + (f" -- WARNING: {bad} not text, leading zeros may be lost" if bad else " (all text, OK)"))
        dates = [n for n, ty in types.items() if ty.startswith("Date")]
        if dates:
            print(f"    date columns: {dates}")
        if not_matched:
            print(f"    note: no display name in the config for {not_matched}")
        if not definition:
            print(f"    layer '{t.properties.name}': name and all {len(in_config)} column display names already set")
            continue
        try:
            t.manager.update_definition(definition)
            print(f"    layer '{t.properties.name}': "
                  + (f"renamed to '{layer_name}', " if "name" in definition else "")
                  + f"{len(to_change)} column display names applied ({len(in_config)} columns matched)")
        except Exception as e:   # cosmetic only -- never fail a publish over it
            print(f"    WARNING: could not apply the layer name / column display names ({e}). The data is fine.")


def display(cfg, page, only=None):
    """Apply titles, layer names and column display names to the existing hosted tables.
    Uploads nothing and changes no data or item IDs."""
    page_cfg = cfg["pages"][page]
    gis = connect_portal(cfg)
    aliases = page_cfg.get("field_aliases", {})
    for name, out in selected_outputs(page_cfg, only):
        if not out.get("portal_item_id"):
            print(f"  {name}: no Portal item yet -- run 'publish --create' first.")
            continue
        print(f"  {name}: {out['portal_item_id']}")
        apply_display_settings(gis, out["portal_item_id"], out, aliases)
    print("[display] done. Item IDs unchanged. Reload the Portal page or the dashboard editor to see the names.")


def check_folder(gis, folder):
    """Stop unless the Portal folder already exists in your own content, spelled exactly as in
    the config. It is never created here: a typo would otherwise quietly put the tables in a new
    folder. Written for every arcgis 2.x (tested with 2.4.3 on Pro 3.7 and 2.4.1 on Pro 3.5)."""
    names = []
    for f in gis.users.me.folders or []:
        names.append(f.get("title") if isinstance(f, dict) else getattr(f, "name", None) or getattr(f, "title", None))
    if folder not in names:
        sys.exit(
            f"[publish] There is no folder '{folder}' in your Portal content. Your folders: {sorted(n for n in names if n)}.\n"
            "          Fix 'folder' under 'portal' in webapp_config.json (exact spelling and capitals), "
            "or create the folder in the Portal first."
        )
    print(f"[publish] Portal folder '{folder}' found")


def create_hosted_table(gis, cfg, page, name, out, csv_path):
    """First run only: upload the CSV and publish it as a hosted TABLE (no geometry)."""
    folder = cfg.get("portal", {}).get("folder") or None
    title = out.get("title") or csv_path.stem.replace("_", " ")
    svc = service_name(title)
    if not gis.content.is_service_name_available(svc, "featureService"):
        sys.exit(
            f"  {name}: the service name '{svc}' is already taken in the Portal -- most likely by the "
            "previous version of this table. Delete that item (see retired_items in webapp_config.json) "
            "and run again."
        )
    print(f"  {name}: creating '{title}'" + (f" in folder '{folder}'" if folder else ""))
    csv_item = gis.content.add(
        {
            "title": f"{title} (source CSV)",
            "type": "CSV",
            "tags": cfg.get("portal", {}).get("tags", "DQIMP"),
            "description": f"Source CSV for {title}. Overwritten by dqimp_webapp.py.",
        },
        data=str(csv_path),
        folder=folder,
    )
    # Let the Portal analyze the CSV, then correct what it guessed: locationType 'none' keeps it
    # a table (no coordinates to geocode), and TEXT_FIELDS are forced to strings.
    params = gis.content.analyze(item=csv_item, file_type="csv")["publishParameters"]
    params["name"] = svc
    params["locationType"] = "none"
    if out.get("layer_name") and "layerInfo" in params:
        params["layerInfo"]["name"] = out["layer_name"]
    for field in params.get("layerInfo", {}).get("fields", []):
        if field.get("name", "").lower() in TEXT_FIELDS_LOWER and field.get("type") != "esriFieldTypeString":
            print(f"    forcing {field['name']} to text (Portal guessed {field.get('type')})")
            field.update(type="esriFieldTypeString", sqlType="sqlTypeNVarchar", length=256)
    hosted = csv_item.publish(publish_parameters=params, file_type="csv")
    out["source_item_id"] = csv_item.id
    out["portal_item_id"] = hosted.id
    save_config(cfg)
    # The hosted table takes the CSV item's title ("... (source CSV)"); its own title is set in
    # apply_display_settings, which publish() calls next.
    print(f"    hosted table {hosted.id} (source CSV item {csv_item.id}) -- ids saved to webapp_config.json")
    return hosted


def overwrite_hosted_table(gis, out, csv_path):
    """Every later run: replace the data in place. The item ID does not change, so every
    dashboard and Web Experience widget built on it keeps working."""
    from arcgis.features import FeatureLayerCollection

    hosted = gis.content.get(out["portal_item_id"])
    if hosted is None:
        sys.exit(f"  Portal item {out['portal_item_id']} not found -- wrong Portal, or the item was deleted.")
    FeatureLayerCollection.fromitem(hosted).manager.overwrite(str(csv_path))
    return hosted


def selected_outputs(page_cfg, only):
    """All outputs of the page, or just the ones named with --only."""
    outputs = page_cfg["outputs"]
    if not only:
        return list(outputs.items())
    unknown = [n for n in only if n not in outputs]
    if unknown:
        sys.exit(f"--only: unknown table(s) {unknown}. This page has: {list(outputs)}")
    return [(n, outputs[n]) for n in only]


def publish(cfg, page, create=False, dry_run=False, only=None):
    page_cfg = cfg["pages"][page]
    out_dir = output_dir(cfg, page)
    print(f"[publish] reading CSVs from {out_dir}")
    plan = []
    for name, out in selected_outputs(page_cfg, only):
        csv_path = out_dir / out["csv"]
        if not csv_path.exists():
            sys.exit(f"[publish] {csv_path} is missing -- run 'build' first.")
        action = "overwrite" if out.get("portal_item_id") else "CREATE"
        plan.append((name, out, csv_path, action))
        print(f"[publish] {name}: {action} from {csv_path.name} ({csv_path.stat().st_size / 1e6:.1f} MB)")

    to_create = [p for p in plan if p[3] == "CREATE"]
    if to_create and not create:
        sys.exit(
            "[publish] These have no Portal item yet: "
            + ", ".join(p[0] for p in to_create)
            + ".\n           Re-run with --create to publish them for the first time. "
            "Creating makes NEW items; check webapp_config.json first so you do not end up with duplicates."
        )
    if to_create:
        print(f"[publish] new tables will go in Portal folder '{cfg.get('portal', {}).get('folder') or '(root)'}'")
    if dry_run:
        print("[publish] --dry-run: nothing sent to the Portal.")
        return

    gis = connect_portal(cfg)
    folder = cfg.get("portal", {}).get("folder")
    if to_create and folder:
        check_folder(gis, folder)   # before anything is uploaded
    aliases = page_cfg.get("field_aliases", {})
    row_mismatches = []
    for name, out, csv_path, action in plan:
        if action == "CREATE":
            try:
                item = create_hosted_table(gis, cfg, page, name, out, csv_path)
            except Exception:
                print(
                    f"\n[publish] {name} FAILED while being created. Before running again, delete any item "
                    f"titled '{out.get('title')} (source CSV)' or '{out.get('title')}' in Portal folder "
                    f"'{cfg.get('portal', {}).get('folder')}': it is incomplete. Tables created before "
                    "this one are saved in webapp_config.json and will simply be overwritten on the rerun.\n"
                )
                raise
        else:
            item = overwrite_hosted_table(gis, out, csv_path)
        apply_display_settings(gis, item.id, out, aliases)
        item = gis.content.get(item.id)   # re-read so the checks below see the applied settings
        tables = (item.tables or []) + (item.layers or [])
        portal_rows = {t.properties.name: t.query(return_count_only=True) for t in tables}
        counts = ", ".join(f"{n}: {c:,} rows" for n, c in portal_rows.items())
        print(f"  {name}: {action} done -- {item.id} ({counts})")
        # Proof that nothing was lost on the way: the Portal table must hold exactly as many rows
        # as the CSV that was sent.
        expected = csv_row_count(csv_path)
        if sum(portal_rows.values()) == expected:
            print(f"    row check passed: Portal {sum(portal_rows.values()):,} = CSV {expected:,}")
        else:
            row_mismatches.append(f"{name}: Portal {sum(portal_rows.values()):,} rows, CSV {expected:,}")
            print(f"    ERROR: row check FAILED -- Portal {sum(portal_rows.values()):,} rows, CSV {expected:,}")
        for t in tables:
            for f in t.properties.fields:
                if f["name"].lower() in TEXT_FIELDS_LOWER and f["type"] != "esriFieldTypeString":
                    print(f"    WARNING: {f['name']} is {f['type']} in the Portal, not text -- leading zeros may be lost.")
    if row_mismatches:
        sys.exit("[publish] STOPPED: a Portal table does not match its CSV -- do not use it. "
                 + "; ".join(row_mismatches))
    if to_create:
        print("[publish] done. New item IDs are in webapp_config.json -- copy that file back to the repo.")
    else:
        print("[publish] done. Item IDs unchanged, so the dashboards update on their own.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["build", "publish", "display"],
                        help="display: apply item titles, layer names and column display names only (no upload)")
    parser.add_argument("--page", default="mlrs_case_completeness")
    parser.add_argument("--output-dir", help="Override the output folder (used when running from a staging copy).")
    parser.add_argument("--create", action="store_true", help="publish: allow creating Portal items that do not exist yet.")
    parser.add_argument("--dry-run", action="store_true", help="publish: show what would happen, send nothing.")
    parser.add_argument("--only", nargs="+", metavar="TABLE",
                        help="publish/display: only these tables (e.g. --only changes). Default: all.")
    args = parser.parse_args()
    cfg = load_config()
    if args.page not in cfg["pages"]:
        sys.exit(f"Unknown page '{args.page}'. Configured: {list(cfg['pages'])}")
    if args.output_dir:
        cfg["_output_dir_override"] = args.output_dir
    if args.command == "build":
        build(cfg, args.page)
    elif args.command == "display":
        display(cfg, args.page, only=args.only)
    else:
        publish(cfg, args.page, create=args.create, dry_run=args.dry_run, only=args.only)


if __name__ == "__main__":
    main()
