#!/usr/bin/env python3
"""
Module Contest & Mid Module Contest Pipeline
Auto-generated cron pipeline — MC + Mid-MC scoring pipelines

Ported from the DS_batches_data notebook, wrapped for unattended GitHub
Actions execution:
  - Auth via env vars / service account instead of Colab's interactive auth.
  - requests.post is patched to use a retry-hardened Session (connection
    resets / 5xx / 429 are retried automatically), matching the fix applied
    to the main Assignment Automation Pipeline for card 9913-style failures.
  - Any uncaught exception exits non-zero so the GitHub Actions run goes red.
"""

import os
import sys
import json
import time
import traceback

import requests
import pandas as pd
import gspread
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

start_time = time.time()

# -------------------- ENV & AUTH --------------------
sec = os.getenv("ASHRITHA_SECRET_KEY")
User_name = os.getenv("METABASE_USERNAME") or os.getenv("USERNAME")
service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")
MB_URL = os.getenv("METABASE_URL")

missing = [n for n, v in [
    ("ASHRITHA_SECRET_KEY", sec),
    ("METABASE_USERNAME/USERNAME", User_name),
    ("SERVICE_ACCOUNT_JSON", service_account_json),
    ("METABASE_URL", MB_URL),
] if not v]
if missing:
    raise ValueError(f"❌ Missing environment variables: {', '.join(missing)}")

service_info = json.loads(service_account_json)
creds = Credentials.from_service_account_info(
    service_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)
gc = gspread.authorize(creds)

METABASE_BASE = "https://metabase-lierhfgoeiwhr.newtonschool.co"

# -------------------- RETRY-HARDENED SESSION --------------------
# Same fix as the main Assignment Automation Pipeline: ConnectionError /
# ECONNRESET, 429, and 5xx are retried at the transport level instead of
# failing the whole job on the first hiccup.
SESSION = requests.Session()
_adapter = HTTPAdapter(
    max_retries=Retry(
        total=4,
        connect=4,
        read=2,
        backoff_factor=5,             # 5s, 10s, 20s, 40s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["POST", "GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    ),
    pool_connections=10,
    pool_maxsize=10,
)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)

# Every requests.post(...) call in the ported notebook code below now goes
# through the retry-hardened session automatically — no need to edit each
# call site individually.
requests.post = SESSION.post

token = None


def refresh_metabase_token():
    global token
    res = SESSION.post(
        MB_URL,
        headers={"Content-Type": "application/json"},
        json={"username": User_name, "password": sec},
        timeout=(15, 60),
    )
    res.raise_for_status()
    token = res.json()["id"]
    print("✅ Metabase session token refreshed")


refresh_metabase_token()

print("🔎 ENV CHECK")
print(f"   MB user           : {'[SET]' if User_name else '[MISSING]'}")
print(f"   SA client_email   : {service_info.get('client_email')}")
print(f"   Token acquired    : {bool(token)}")

# ═══════════════════════════════════════════════════════════════════════════
# PIPELINE BODY (ported from notebook cells: 17,18,21,22,23)
# ═══════════════════════════════════════════════════════════════════════════
try:
    # ── MODULE CONTEST ──────────────────────────────────────────────
    # ──────────────────────────────────────────────────────────────────────
    # Cell 17 — canonical version (includes gem_label) — cell 19's near-duplicate without gem_label was dropped, see README
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import gspread
    from gspread_dataframe import set_with_dataframe
    import requests

    # ═══════════════════════════════════════════════════════════════════════════
    # HELPER
    # ═══════════════════════════════════════════════════════════════════════════
    def clean_to_int(series):
        return pd.to_numeric(series.astype(str)
                             .str.replace(',', '')
                             .str.replace(r'\.0$', '', regex=True)
                             .str.strip(),
                             errors='coerce').fillna(0).astype(int)

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 0: GROOMERS FILTER (approved / active students only)
    # ═══════════════════════════════════════════════════════════════════════════
    print("Reading Groomers...")
    workbook = gc.open('Groomers')
    worksheet1 = workbook.worksheet('Groomers')
    data1 = worksheet1.get_all_values()
    df_groomers = pd.DataFrame(data1)

    df_groomers.columns = df_groomers.iloc[0]
    df_groomers = df_groomers.iloc[1:].copy()
    df_groomers = df_groomers.rename(columns={'UserID': 'user_id'})

    filtered_groomers = df_groomers[
        (df_groomers['Enrolled Status'] != 'Refund Requested') &
        (df_groomers['Phase'] != 'Unavailable') &
        (df_groomers['Enrolled Status'] != 'DPD/Foreclosed')
    ].copy()

    filtered_groomers['user_id'] = clean_to_int(filtered_groomers['user_id'])
    allowed_ids = filtered_groomers['user_id'].unique().tolist()
    print(f"✓ Groomers: {len(allowed_ids)} approved user_ids")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 1: MC RAW DATA
    # ═══════════════════════════════════════════════════════════════════════════
    workbook = gc.open('Placements')
    worksheet1 = workbook.worksheet('MC_Raw_2')
    data1 = worksheet1.get_all_values()
    df = pd.DataFrame(data1)

    df.columns = df.iloc[0]
    df = df.iloc[1:].copy()
    df['user_id'] = clean_to_int(df['user_id'])

    # Apply Groomers filter
    before = len(df)
    df = df[df['user_id'].isin(allowed_ids)].copy()
    print(f"✓ MC raw: {len(df)} rows after Groomers filter (dropped {before - len(df)})")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 2: METABASE AU DATA (brings au_start_date)
    # ═══════════════════════════════════════════════════════════════════════════
    res3 = requests.post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6289/query/json',
                         headers={'Content-Type': 'application/json',
                                  'X-Metabase-Session': token},
                         timeout=3600)
    df_au = pd.DataFrame(res3.json())
    df_au = df_au[['user_id', 'label', 'au_batch_name', 'au_start_date','gem_label']]
    # df_au = df_au[df_au['label'].isin(['Enrolled'])]

    df_au['user_id'] = clean_to_int(df_au['user_id'])
    df_au = df_au.rename(columns={'au_batch_name': 'admin_unit_name'})

    df = pd.merge(df, df_au, on=['user_id', 'admin_unit_name'], how='inner')
    print(f"✓ After AU merge: {len(df)} rows")

    # Exclude "Advantage" admin units
    before = len(df)
    df = df[~df['admin_unit_name'].str.contains('advantage', case=False, na=False)].copy()
    print(f"✓ Dropped {before - len(df)} rows with 'Advantage' in admin_unit_name")

    # Clean Scores and Dates
    df['Total Score'] = pd.to_numeric(df['Total Score'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    df['contest_date'] = pd.to_datetime(df['contest_date'])
    df['au_start_date'] = pd.to_datetime(df['au_start_date'], errors='coerce')

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 2b: METABASE BATCH STATS (enrolled / refund / cancellation / deferred)
    # ═══════════════════════════════════════════════════════════════════════════
    res_bs = requests.post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/11855/query/json',
                           headers={'Content-Type': 'application/json',
                                    'X-Metabase-Session': token},
                           timeout=3600)
    df_bs = pd.DataFrame(res_bs.json())
    print("Batch-stats columns:", list(df_bs.columns))   # verify names once, then you can remove

    stat_cols = ['currently_enrolled', 'refund_requested_students',
                 'course_cancellation', 'initially_enrolled', 'deferred']
    for c in stat_cols:
        df_bs[c] = clean_to_int(df_bs[c])

    # one row per batch (mirrors the SUM ... GROUP BY au_batch_name in the SQL)
    df_bs = (df_bs.groupby('au_batch_name', as_index=False)[stat_cols].sum()
                  .rename(columns={'au_batch_name': 'Admin_Unit_name'}))
    print(f"✓ Batch stats: {len(df_bs)} batches")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3: WEEK START DATE (Monday)
    # ═══════════════════════════════════════════════════════════════════════════
    df['Week_Start_Date'] = df['contest_date'] - pd.to_timedelta(df['contest_date'].dt.weekday, unit='D')

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3b: DROP DUPLICATE-DATE ROWS, KEEPING THE HIGHEST SCORE
    # ═══════════════════════════════════════════════════════════════════════════
    before = len(df)
    df = df.sort_values(['user_id', 'admin_unit_name', 'module_name', 'contest_date', 'Total Score'],
                        ascending=[True, True, True, True, False])
    df = df.drop_duplicates(subset=['user_id', 'admin_unit_name', 'module_name', 'contest_date'], keep='first')
    print(f"Dropped {before - len(df)} duplicate-date rows (kept highest score each time)")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3c: MERGE ROWS THAT ARE 1 DAY APART
    # ═══════════════════════════════════════════════════════════════════════════
    df = df.sort_values(['user_id', 'admin_unit_name', 'module_name', 'contest_date'])

    merged_rows = []
    for _, g in df.groupby(['user_id', 'admin_unit_name', 'module_name'], sort=False):
        g = g.sort_values('contest_date').reset_index(drop=True)
        cluster_start = 0
        for i in range(1, len(g) + 1):
            if i == len(g) or (g.loc[i, 'contest_date'] - g.loc[i - 1, 'contest_date']).days > 1:
                cluster = g.loc[cluster_start:i - 1]
                best = cluster.loc[cluster['Total Score'].idxmax()]
                merged_rows.append(best)
                cluster_start = i

    before_merge = len(df)
    df = pd.DataFrame(merged_rows).reset_index(drop=True)
    print(f"Merged {before_merge - len(df)} rows that were 1 day apart")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 4: HIGHEST SCORE / ATTEMPT NUMBER / STATUS
    # ═══════════════════════════════════════════════════════════════════════════
    df['Highest_Score_Overall'] = df.groupby(['user_id', 'admin_unit_name', 'module_name'])['Total Score'].transform('max')

    df = df.sort_values(['user_id', 'module_name', 'contest_date'])
    df['Attempt_number'] = df.groupby(['user_id', 'module_name']).cumcount() + 1

    threshold = 64
    df['Status'] = df['Total Score'].apply(lambda x: 'Cleared' if x >= threshold else 'Not Cleared')

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 5: FINAL REPORT (au_start_date retained)
    # ═══════════════════════════════════════════════════════════════════════════
    final_report = df[[
        'user_id',
        'admin_unit_name',
        'au_start_date',
         'gem_label',
        'module_name',
        'Attempt_number',
        'contest_date',
        'Week_Start_Date',
        'Total Score',
        'Highest_Score_Overall',
        'Status'
    ]].copy()

    final_report['contest_date'] = final_report['contest_date'].dt.strftime('%Y-%m-%d')
    final_report['Week_Start_Date'] = final_report['Week_Start_Date'].dt.strftime('%Y-%m-%d')
    final_report['au_start_date'] = final_report['au_start_date'].dt.strftime('%Y-%m-%d')

    Module_Attempt_wise = final_report.rename(columns={
        'admin_unit_name': 'Admin_Unit_name',
        'contest_date': 'contest_date_MC',
        'Attempt_number': 'Attempt_no_MC',
        'Total Score': 'Total_Score_MC',
        'Highest_Score_Overall': 'Highest_Score_MC',
        'Status': 'Status_MC'
    })

    Module_Attempt_wise = Module_Attempt_wise[[
        'user_id', 'Admin_Unit_name', 'au_start_date','gem_label', 'module_name', 'Attempt_no_MC',
        'contest_date_MC', 'Week_Start_Date', 'Total_Score_MC',
        'Highest_Score_MC', 'Status_MC'
    ]]

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 6: MERGE WITH MASTER DATA
    # ═══════════════════════════════════════════════════════════════════════════
    print("Merging with Master Data...")
    workbook = gc.open('DS Full program - All Intake 2026')
    worksheet1 = workbook.worksheet('Master Data 2023-2026')
    data1 = worksheet1.get_all_values()
    df_master = pd.DataFrame(data1)

    df_master.columns = df_master.iloc[0]
    df_master = df_master.iloc[1:].copy()
    df_master = df_master.rename(columns={'User ID ': 'user_id'})
    df_master = df_master[~df_master['Persona'].isin(['NF', '#N/A']) & df_master['Persona'].notna()].copy()
    df_master['user_id'] = clean_to_int(df_master['user_id'])

    Module_Attempt_wise['user_id'] = clean_to_int(Module_Attempt_wise['user_id'])

    df_MC = pd.merge(Module_Attempt_wise, df_master, on='user_id', how='left')
    print(f"✓ Merged data: {len(df_MC)} rows")

    # merge batch-level stats onto the student rows (constant per batch)
    df_MC = pd.merge(df_MC, df_bs, on='Admin_Unit_name', how='left')
    print(f"✓ After batch-stats merge: {len(df_MC)} rows")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 7: WRITE TO GOOGLE SHEETS
    # ═══════════════════════════════════════════════════════════════════════════
    sheet = gc.open_by_key('1I4HAAkbZl2Zr6IblLRh1AasfBj1LbZCj0bQ-X1wLGFM')
    worksheet = sheet.worksheet("Student-level-MC")
    worksheet.clear()
    set_with_dataframe(worksheet, df_MC, include_index=False, include_column_header=True)

    print("\n" + "=" * 60)
    print("✅ UPLOAD SUCCESSFUL")
    print(f"Rows: {len(df_MC)} | Columns: {len(df_MC.columns)}")
    print("=" * 60)

    # ──────────────────────────────────────────────────────────────────────
    # Cell 18 — Attempt-2 value-add view, depends on cell 17's output
    # ──────────────────────────────────────────────────────────────────────
    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 8: ATTEMPT-2 VALUE-ADD RAW VIEW (MC)  → Looker
    # Single contest, multiple sittings. "Attempt 1/2" = 1st/2nd sitting of the
    # SAME contest. Sittings 3+ are ignored for this A1-vs-A2 view.
    # Run AFTER Module_Attempt_wise (Step 5) exists.
    # ═══════════════════════════════════════════════════════════════════════════
    THRESHOLD = 64          # keep in sync with Step 4 'Status'

    aw = Module_Attempt_wise.copy()
    aw['Total_Score_MC'] = pd.to_numeric(aw['Total_Score_MC'], errors='coerce')
    key = ['user_id', 'Admin_Unit_name', 'module_name']

    a1 = (aw[aw['Attempt_no_MC'] == 1]
          .rename(columns={'contest_date_MC': 'A1_date',
                           'Total_Score_MC':  'A1_Score',
                           'Status_MC':       'A1_Status'})
          [key + ['A1_date', 'A1_Score', 'A1_Status']].drop_duplicates(key))

    a2 = (aw[aw['Attempt_no_MC'] == 2]
          .rename(columns={'contest_date_MC': 'A2_date',
                           'Total_Score_MC':  'A2_Score',
                           'Status_MC':       'A2_Status'})
          [key + ['au_start_date', 'A2_date', 'Week_Start_Date',
                  'A2_Score', 'A2_Status']].drop_duplicates(key))

    # Universe = every student-module that reached a 2nd sitting.
    view = a2.merge(a1, on=key, how='inner')

    # ---- classify (2 groups) -----------------------------------------------------
    view['Category'] = view['A1_Status'].map({'Cleared':     'Improver (cleared A1)',
                                              'Not Cleared': 'Retrier (failed A1)'})

    # cross-cutting flags = the dashboard's Cleared / Improved columns
    view['Cleared_A2']     = (view['A2_Score'] >= THRESHOLD).map({True: 'Yes', False: 'No'})
    view['Improved_vs_A1'] = (view['A2_Score'] > view['A1_Score']).map({True: 'Yes', False: 'No'})
    view['Score_Delta']    = view['A2_Score'] - view['A1_Score']

    # mutually-exclusive outcome (for a single pie/bar)
    def outcome(r):
        if r['A1_Status'] == 'Cleared':                    return 'Already cleared (A1)'
        return 'Recovered on A2' if r['Cleared_A2'] == 'Yes' else 'Still failing'
    view['Outcome_Segment'] = view.apply(outcome, axis=1)

    # Month bucket for the time-series
    view['A2_date']     = pd.to_datetime(view['A2_date'], errors='coerce')
    view['Month']       = view['A2_date'].dt.to_period('M').dt.to_timestamp()
    view['Month_Label'] = view['A2_date'].dt.strftime('%b %Y')

    # ---- order + date formatting -------------------------------------------------
    view['Category_Order'] = view['Category'].map({'Retrier (failed A1)': 1,
                                                   'Improver (cleared A1)': 2})
    view = view.sort_values(['Month', 'Category_Order', 'user_id'])
    for c in ['A1_date', 'A2_date', 'Week_Start_Date', 'au_start_date', 'Month']:
        view[c] = pd.to_datetime(view[c], errors='coerce').dt.strftime('%Y-%m-%d')

    print(f"MC Attempt-2 universe: {len(view)} student-modules")
    print(view['Category'].value_counts().to_string())
    print(view['Outcome_Segment'].value_counts().to_string())

    # ---- batch-stats merge (constant per batch; from df_bs built in Step 2b) -----
    view = view.merge(df_bs, on='Admin_Unit_name', how='left')

    # ---- master merge + write ----------------------------------------------------
    view['user_id'] = clean_to_int(view['user_id'])
    view = view.merge(df_master, on='user_id', how='left')

    out_tab = "MC-Attempt2-ValueAdd-Raw"     # distinct tab so it won't overwrite the Mid-MC view
    sheet = gc.open_by_key('1I4HAAkbZl2Zr6IblLRh1AasfBj1LbZCj0bQ-X1wLGFM')
    try:
        ws = sheet.worksheet(out_tab)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=out_tab, rows=2, cols=2)
    ws.clear()
    set_with_dataframe(ws, view, include_index=False, include_column_header=True)
    print(f"✅ Wrote {len(view)} rows → '{out_tab}'")

    # ── MID MODULE CONTEST ──────────────────────────────────────────
    # ──────────────────────────────────────────────────────────────────────
    # Cell 21
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import gspread
    from gspread_dataframe import set_with_dataframe
    import requests

    # ═══════════════════════════════════════════════════════════════════════════
    # HELPER
    # ═══════════════════════════════════════════════════════════════════════════
    def clean_to_int(series):
        return pd.to_numeric(series.astype(str)
                             .str.replace(',', '')
                             .str.replace(r'\.0$', '', regex=True)
                             .str.strip(),
                             errors='coerce').fillna(0).astype(int)

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 0: GROOMERS FILTER (approved / active students only)
    # ═══════════════════════════════════════════════════════════════════════════
    print("Reading Groomers...")
    workbook = gc.open('Groomers')
    worksheet1 = workbook.worksheet('Groomers')
    data1 = worksheet1.get_all_values()
    df_groomers = pd.DataFrame(data1)

    df_groomers.columns = df_groomers.iloc[0]
    df_groomers = df_groomers.iloc[1:].copy()
    df_groomers = df_groomers.rename(columns={'UserID': 'user_id'})

    filtered_groomers = df_groomers[
        (df_groomers['Enrolled Status'] != 'Refund Requested') &
        (df_groomers['Phase'] != 'Unavailable') &
        (df_groomers['Enrolled Status'] != 'DPD/Foreclosed')
    ].copy()

    filtered_groomers['user_id'] = clean_to_int(filtered_groomers['user_id'])
    allowed_ids = filtered_groomers['user_id'].unique().tolist()
    print(f"✓ Groomers: {len(allowed_ids)} approved user_ids")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 1: MID-MC RAW DATA
    # ═══════════════════════════════════════════════════════════════════════════
    workbook = gc.open('Placements')
    worksheet1 = workbook.worksheet('Mid_MC_Raw')
    data1 = worksheet1.get_all_values()
    df = pd.DataFrame(data1)

    df.columns = df.iloc[0]
    df = df.iloc[1:].copy()
    df['user_id'] = clean_to_int(df['user_id'])

    # Apply Groomers filter
    before = len(df)
    df = df[df['user_id'].isin(allowed_ids)].copy()
    print(f"✓ Mid-MC raw: {len(df)} rows after Groomers filter (dropped {before - len(df)})")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 2: METABASE AU DATA (brings au_start_date)
    # ═══════════════════════════════════════════════════════════════════════════
    res3 = requests.post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6289/query/json',
                         headers={'Content-Type': 'application/json',
                                  'X-Metabase-Session': token},
                         timeout=3600)
    df_au = pd.DataFrame(res3.json())
    df_au = df_au[['user_id', 'label', 'au_batch_name', 'au_start_date']]
    # df_au = df_au[df_au['label'].isin(['Enrolled'])]

    df_au['user_id'] = clean_to_int(df_au['user_id'])
    df_au = df_au.rename(columns={'au_batch_name': 'admin_unit_name'})

    df = pd.merge(df, df_au, on=['user_id', 'admin_unit_name'], how='inner')
    print(f"✓ After AU merge: {len(df)} rows")

    # Exclude "Advantage" admin units
    before = len(df)
    df = df[~df['admin_unit_name'].str.contains('advantage', case=False, na=False)].copy()
    print(f"✓ Dropped {before - len(df)} rows with 'Advantage' in admin_unit_name")

    # Clean Total Score and Dates
    df['Total Score'] = pd.to_numeric(df['Total Score'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    df['contest_date'] = pd.to_datetime(df['contest_date'])
    df['au_start_date'] = pd.to_datetime(df['au_start_date'], errors='coerce')

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 2b: METABASE BATCH STATS (enrolled / refund / cancellation / deferred)
    # ═══════════════════════════════════════════════════════════════════════════
    res_bs = requests.post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/11855/query/json',
                           headers={'Content-Type': 'application/json',
                                    'X-Metabase-Session': token},
                           timeout=3600)
    df_bs = pd.DataFrame(res_bs.json())
    print("Batch-stats columns:", list(df_bs.columns))   # verify names once, then you can remove

    stat_cols = ['currently_enrolled', 'refund_requested_students',
                 'course_cancellation', 'initially_enrolled', 'deferred']
    for c in stat_cols:
        df_bs[c] = clean_to_int(df_bs[c])

    # one row per batch (mirrors the SUM ... GROUP BY au_batch_name in the SQL)
    df_bs = (df_bs.groupby('au_batch_name', as_index=False)[stat_cols].sum()
                  .rename(columns={'au_batch_name': 'Admin_Unit_name'}))
    print(f"✓ Batch stats: {len(df_bs)} batches")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3: WEEK START DATE (Monday)
    # ═══════════════════════════════════════════════════════════════════════════
    df['Week_Start_Date'] = df['contest_date'] - pd.to_timedelta(df['contest_date'].dt.weekday, unit='D')

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3b: DROP DUPLICATE-DATE ROWS, KEEPING THE HIGHEST SCORE
    # ═══════════════════════════════════════════════════════════════════════════
    before = len(df)
    df = df.sort_values(['user_id', 'admin_unit_name', 'module_name', 'contest_date', 'Total Score'],
                        ascending=[True, True, True, True, False])
    df = df.drop_duplicates(subset=['user_id', 'admin_unit_name', 'module_name', 'contest_date'], keep='first')
    print(f"Dropped {before - len(df)} duplicate-date rows (kept highest score each time)")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3c: MERGE ROWS THAT ARE 1 DAY APART
    # ═══════════════════════════════════════════════════════════════════════════
    df = df.sort_values(['user_id', 'admin_unit_name', 'module_name', 'contest_date'])

    merged_rows = []
    for _, g in df.groupby(['user_id', 'admin_unit_name', 'module_name'], sort=False):
        g = g.sort_values('contest_date').reset_index(drop=True)
        cluster_start = 0
        for i in range(1, len(g) + 1):
            if i == len(g) or (g.loc[i, 'contest_date'] - g.loc[i - 1, 'contest_date']).days > 1:
                cluster = g.loc[cluster_start:i - 1]
                best = cluster.loc[cluster['Total Score'].idxmax()]
                merged_rows.append(best)
                cluster_start = i

    before_merge = len(df)
    df = pd.DataFrame(merged_rows).reset_index(drop=True)
    print(f"Merged {before_merge - len(df)} rows that were 1 day apart")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3d: CAP AT 2 ATTEMPTS MAX (per user + module)
    # ═══════════════════════════════════════════════════════════════════════════
    df = df.sort_values(['user_id', 'module_name', 'contest_date'])
    before = len(df)
    df = df[df.groupby(['user_id', 'module_name']).cumcount() < 2]
    print(f"Dropped {before - len(df)} rows beyond the 2-attempt cap")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 4: HIGHEST SCORE / ATTEMPT NUMBER / STATUS
    # ═══════════════════════════════════════════════════════════════════════════
    df['Highest_Score_Overall'] = df.groupby(['user_id', 'admin_unit_name', 'module_name'])['Total Score'].transform('max')

    df = df.sort_values(['user_id', 'module_name', 'contest_date'])
    df['Attempt_number'] = df.groupby(['user_id', 'module_name']).cumcount() + 1

    threshold = 64
    df['Status'] = df['Total Score'].apply(lambda x: 'Cleared' if x >= threshold else 'Not Cleared')

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 5: FINAL REPORT (au_start_date retained)
    # ═══════════════════════════════════════════════════════════════════════════
    final_report = df[[
        'user_id',
        'admin_unit_name',
        'au_start_date',
        'module_name',
        'Attempt_number',
        'contest_date',
        'Week_Start_Date',
        'Total Score',
        'Highest_Score_Overall',
        'Status'
    ]].copy()

    final_report['contest_date'] = final_report['contest_date'].dt.strftime('%Y-%m-%d')
    final_report['Week_Start_Date'] = final_report['Week_Start_Date'].dt.strftime('%Y-%m-%d')
    final_report['au_start_date'] = final_report['au_start_date'].dt.strftime('%Y-%m-%d')

    Mid_Module_Attempt_wise = final_report.rename(columns={
        'admin_unit_name': 'Admin_Unit_name',
        'contest_date': 'contest_date_Mid_MC',
        'Attempt_number': 'Attempt_no_Mid_MC',
        'Total Score': 'Total_Score_Mid_MC',
        'Highest_Score_Overall': 'Highest_Score_Mid_MC',
        'Status': 'Status_Mid_MC'
    })

    Mid_Module_Attempt_wise = Mid_Module_Attempt_wise[[
        'user_id', 'Admin_Unit_name', 'au_start_date', 'module_name', 'Attempt_no_Mid_MC',
        'contest_date_Mid_MC', 'Week_Start_Date', 'Total_Score_Mid_MC',
        'Highest_Score_Mid_MC', 'Status_Mid_MC'
    ]]

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 6: MERGE WITH MASTER DATA
    # ═══════════════════════════════════════════════════════════════════════════
    print("Merging with Master Data...")
    workbook = gc.open('DS Full program - All Intake 2026')
    worksheet1 = workbook.worksheet('Master Data 2023-2026')
    data1 = worksheet1.get_all_values()
    df_master = pd.DataFrame(data1)

    df_master.columns = df_master.iloc[0]
    df_master = df_master.iloc[1:].copy()
    df_master = df_master.rename(columns={'User ID ': 'user_id'})
    df_master = df_master[~df_master['Persona'].isin(['NF', '#N/A']) & df_master['Persona'].notna()].copy()
    df_master['user_id'] = clean_to_int(df_master['user_id'])

    Mid_Module_Attempt_wise['user_id'] = clean_to_int(Mid_Module_Attempt_wise['user_id'])

    df_mid_MC = pd.merge(Mid_Module_Attempt_wise, df_master, on='user_id', how='left')
    print(f"✓ Merged data: {len(df_mid_MC)} rows")

    # merge batch-level stats onto the student rows (constant per batch)
    df_mid_MC = pd.merge(df_mid_MC, df_bs, on='Admin_Unit_name', how='left')
    print(f"✓ After batch-stats merge: {len(df_mid_MC)} rows")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 7: WRITE TO GOOGLE SHEETS
    # ═══════════════════════════════════════════════════════════════════════════
    sheet = gc.open_by_key('1I4HAAkbZl2Zr6IblLRh1AasfBj1LbZCj0bQ-X1wLGFM')
    worksheet = sheet.worksheet("Student-level-Mid-MC")
    worksheet.clear()
    set_with_dataframe(worksheet, df_mid_MC, include_index=False, include_column_header=True)

    print("\n" + "=" * 60)
    print("✅ UPLOAD SUCCESSFUL")
    print(f"Rows: {len(df_mid_MC)} | Columns: {len(df_mid_MC.columns)}")
    print("=" * 60)

    # ──────────────────────────────────────────────────────────────────────
    # Cell 22 — Attempt-2 value-add view, depends on cell 21's output
    # ──────────────────────────────────────────────────────────────────────
    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 8: ATTEMPT-2 VALUE-ADD RAW VIEW  → Looker
    # Single contest, two sittings. "Attempt 1/2" = 1st/2nd sitting of the SAME
    # contest. No A1/A2 rounds, so "New (missed A1)" is impossible and omitted.
    # Run AFTER Mid_Module_Attempt_wise (Step 5) exists.
    # ═══════════════════════════════════════════════════════════════════════════
    THRESHOLD = 64          # keep in sync with Step 4 'Status'

    # ─── Batch stats (enrolled / refund / cancellation / deferred) ───────────────
    res_bs = requests.post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/11855/query/json',
                           headers={'Content-Type': 'application/json',
                                    'X-Metabase-Session': token},
                           timeout=3600)
    df_bs = pd.DataFrame(res_bs.json())
    print("Batch-stats columns:", list(df_bs.columns))   # verify names once, then you can remove

    stat_cols = ['currently_enrolled', 'refund_requested_students',
                 'course_cancellation', 'initially_enrolled', 'deferred']
    for c in stat_cols:
        df_bs[c] = clean_to_int(df_bs[c])

    # one row per batch (mirrors the SUM ... GROUP BY au_batch_name in the SQL)
    df_bs = (df_bs.groupby('au_batch_name', as_index=False)[stat_cols].sum()
                  .rename(columns={'au_batch_name': 'Admin_Unit_name'}))
    print(f"✓ Batch stats: {len(df_bs)} batches")

    aw = Mid_Module_Attempt_wise.copy()
    aw['Total_Score_Mid_MC'] = pd.to_numeric(aw['Total_Score_Mid_MC'], errors='coerce')
    key = ['user_id', 'Admin_Unit_name', 'module_name']

    # Attempt round = chronological sitting number (already correct for one contest)
    a1 = (aw[aw['Attempt_no_Mid_MC'] == 1]
          .rename(columns={'contest_date_Mid_MC': 'A1_date',
                           'Total_Score_Mid_MC':  'A1_Score',
                           'Status_Mid_MC':       'A1_Status'})
          [key + ['A1_date', 'A1_Score', 'A1_Status']].drop_duplicates(key))

    a2 = (aw[aw['Attempt_no_Mid_MC'] == 2]
          .rename(columns={'contest_date_Mid_MC': 'A2_date',
                           'Total_Score_Mid_MC':  'A2_Score',
                           'Status_Mid_MC':       'A2_Status'})
          [key + ['au_start_date', 'A2_date', 'Week_Start_Date',
                  'A2_Score', 'A2_Status']].drop_duplicates(key))

    # Universe = every student-module that reached a 2nd sitting.
    # inner join is safe here: a 2nd sitting always has a 1st.
    view = a2.merge(a1, on=key, how='inner')

    # ---- classify (2 groups only) ------------------------------------------------
    view['Category'] = view['A1_Status'].map({'Cleared':     'Improver (cleared A1)',
                                              'Not Cleared': 'Retrier (failed A1)'})

    # cross-cutting flags — these are the dashboard's Cleared / Improved columns
    view['Cleared_A2']     = (view['A2_Score'] >= THRESHOLD).map({True: 'Yes', False: 'No'})
    view['Improved_vs_A1'] = (view['A2_Score'] > view['A1_Score']).map({True: 'Yes', False: 'No'})
    view['Score_Delta']    = view['A2_Score'] - view['A1_Score']

    # Outcome segment (mutually exclusive — for a single pie/bar if you want one)
    def outcome(r):
        if r['A1_Status'] == 'Cleared':                    return 'Already cleared (A1)'
        return 'Recovered on A2' if r['Cleared_A2'] == 'Yes' else 'Still failing'
    view['Outcome_Segment'] = view.apply(outcome, axis=1)

    # Month bucket for the Jan/Feb/Mar time-series
    view['A2_date']     = pd.to_datetime(view['A2_date'], errors='coerce')
    view['Month']       = view['A2_date'].dt.to_period('M').dt.to_timestamp()
    view['Month_Label'] = view['A2_date'].dt.strftime('%b %Y')

    # ---- order + date formatting -------------------------------------------------
    view['Category_Order'] = view['Category'].map({'Retrier (failed A1)': 1,
                                                   'Improver (cleared A1)': 2})
    view = view.sort_values(['Month', 'Category_Order', 'user_id'])
    for c in ['A1_date', 'A2_date', 'Week_Start_Date', 'au_start_date', 'Month']:
        view[c] = pd.to_datetime(view[c], errors='coerce').dt.strftime('%Y-%m-%d')

    print(f"Attempt-2 universe: {len(view)} student-modules")
    print(view['Category'].value_counts().to_string())
    print(view['Outcome_Segment'].value_counts().to_string())

    # ---- master merge + batch-stats merge + write --------------------------------
    view['user_id'] = clean_to_int(view['user_id'])
    view = view.merge(df_master, on='user_id', how='left')

    # merge batch-level stats onto the student rows (constant per batch)
    view = view.merge(df_bs, on='Admin_Unit_name', how='left')

    out_tab = "Attempt2-ValueAdd-Raw"
    sheet = gc.open_by_key('1I4HAAkbZl2Zr6IblLRh1AasfBj1LbZCj0bQ-X1wLGFM')
    try:
        ws = sheet.worksheet(out_tab)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=out_tab, rows=2, cols=2)
    ws.clear()
    set_with_dataframe(ws, view, include_index=False, include_column_header=True)
    print(f"✅ Wrote {len(view)} rows → '{out_tab}'")

    # ──────────────────────────────────────────────────────────────────────
    # Cell 23 — Head-to-head student-wise view
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import gspread
    from gspread_dataframe import set_with_dataframe
    import requests

    # ═══════════════════════════════════════════════════════════════════════════
    # HELPER
    # ═══════════════════════════════════════════════════════════════════════════
    def clean_to_int(series):
        return pd.to_numeric(series.astype(str)
                             .str.replace(',', '')
                             .str.replace(r'\.0$', '', regex=True)
                             .str.strip(),
                             errors='coerce').fillna(0).astype(int)

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 0: GROOMERS FILTER (approved / active students only)
    # ═══════════════════════════════════════════════════════════════════════════
    print("Reading Groomers...")
    workbook = gc.open('Groomers')
    worksheet1 = workbook.worksheet('Groomers')
    data1 = worksheet1.get_all_values()
    df_groomers = pd.DataFrame(data1)

    df_groomers.columns = df_groomers.iloc[0]
    df_groomers = df_groomers.iloc[1:].copy()
    df_groomers = df_groomers.rename(columns={'UserID': 'user_id'})

    filtered_groomers = df_groomers[
        (df_groomers['Enrolled Status'] != 'Refund Requested') &
        (df_groomers['Phase'] != 'Unavailable') &
        (df_groomers['Enrolled Status'] != 'DPD/Foreclosed')
    ].copy()

    filtered_groomers['user_id'] = clean_to_int(filtered_groomers['user_id'])
    allowed_ids = filtered_groomers['user_id'].unique().tolist()
    print(f"✓ Groomers: {len(allowed_ids)} approved user_ids")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 1: MID-MC RAW DATA
    # ═══════════════════════════════════════════════════════════════════════════
    workbook = gc.open('Placements')
    worksheet1 = workbook.worksheet('Mid_MC_Raw')
    data1 = worksheet1.get_all_values()
    df = pd.DataFrame(data1)

    df.columns = df.iloc[0]
    df = df.iloc[1:].copy()
    df['user_id'] = clean_to_int(df['user_id'])

    # Apply Groomers filter
    before = len(df)
    df = df[df['user_id'].isin(allowed_ids)].copy()
    print(f"✓ Mid-MC raw: {len(df)} rows after Groomers filter (dropped {before - len(df)})")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 2: METABASE AU DATA (brings au_start_date)
    # ═══════════════════════════════════════════════════════════════════════════
    res3 = requests.post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6289/query/json',
                         headers={'Content-Type': 'application/json',
                                  'X-Metabase-Session': token},
                         timeout=3600)
    df_au = pd.DataFrame(res3.json())
    df_au = df_au[['user_id', 'label', 'au_batch_name', 'au_start_date']]
    # df_au = df_au[df_au['label'].isin(['Enrolled'])]

    df_au['user_id'] = clean_to_int(df_au['user_id'])
    df_au = df_au.rename(columns={'au_batch_name': 'admin_unit_name'})

    df = pd.merge(df, df_au, on=['user_id', 'admin_unit_name'], how='inner')
    print(f"✓ After AU merge: {len(df)} rows")

    # Exclude "Advantage" admin units
    before = len(df)
    df = df[~df['admin_unit_name'].str.contains('advantage', case=False, na=False)].copy()
    print(f"✓ Dropped {before - len(df)} rows with 'Advantage' in admin_unit_name")

    # Clean Total Score and Dates
    df['Total Score'] = pd.to_numeric(df['Total Score'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    df['contest_date'] = pd.to_datetime(df['contest_date'])
    df['au_start_date'] = pd.to_datetime(df['au_start_date'], errors='coerce')

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 2b: METABASE BATCH STATS (enrolled / refund / cancellation / deferred)
    # ═══════════════════════════════════════════════════════════════════════════
    res_bs = requests.post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/11855/query/json',
                           headers={'Content-Type': 'application/json',
                                    'X-Metabase-Session': token},
                           timeout=3600)
    df_bs = pd.DataFrame(res_bs.json())
    print("Batch-stats columns:", list(df_bs.columns))   # verify names once, then you can remove

    stat_cols = ['currently_enrolled', 'refund_requested_students',
                 'course_cancellation', 'initially_enrolled', 'deferred']
    for c in stat_cols:
        df_bs[c] = clean_to_int(df_bs[c])

    # one row per batch (mirrors the SUM ... GROUP BY au_batch_name in the SQL)
    df_bs = (df_bs.groupby('au_batch_name', as_index=False)[stat_cols].sum()
                  .rename(columns={'au_batch_name': 'Admin_Unit_name'}))
    print(f"✓ Batch stats: {len(df_bs)} batches")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3: WEEK START DATE (Monday)
    # ═══════════════════════════════════════════════════════════════════════════
    df['Week_Start_Date'] = df['contest_date'] - pd.to_timedelta(df['contest_date'].dt.weekday, unit='D')

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3b: DROP DUPLICATE-DATE ROWS, KEEPING THE HIGHEST SCORE
    # ═══════════════════════════════════════════════════════════════════════════
    before = len(df)
    df = df.sort_values(['user_id', 'admin_unit_name', 'module_name', 'contest_date', 'Total Score'],
                        ascending=[True, True, True, True, False])
    df = df.drop_duplicates(subset=['user_id', 'admin_unit_name', 'module_name', 'contest_date'], keep='first')
    print(f"Dropped {before - len(df)} duplicate-date rows (kept highest score each time)")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3c: MERGE ROWS THAT ARE 1 DAY APART
    # ═══════════════════════════════════════════════════════════════════════════
    df = df.sort_values(['user_id', 'admin_unit_name', 'module_name', 'contest_date'])

    merged_rows = []
    for _, g in df.groupby(['user_id', 'admin_unit_name', 'module_name'], sort=False):
        g = g.sort_values('contest_date').reset_index(drop=True)
        cluster_start = 0
        for i in range(1, len(g) + 1):
            if i == len(g) or (g.loc[i, 'contest_date'] - g.loc[i - 1, 'contest_date']).days > 1:
                cluster = g.loc[cluster_start:i - 1]
                best = cluster.loc[cluster['Total Score'].idxmax()]
                merged_rows.append(best)
                cluster_start = i

    before_merge = len(df)
    df = pd.DataFrame(merged_rows).reset_index(drop=True)
    print(f"Merged {before_merge - len(df)} rows that were 1 day apart")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3d: CAP AT 2 ATTEMPTS MAX (per user + module)
    # ═══════════════════════════════════════════════════════════════════════════
    df = df.sort_values(['user_id', 'module_name', 'contest_date'])
    before = len(df)
    df = df[df.groupby(['user_id', 'module_name']).cumcount() < 2]
    print(f"Dropped {before - len(df)} rows beyond the 2-attempt cap")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 4: HIGHEST SCORE / ATTEMPT NUMBER / STATUS
    # ═══════════════════════════════════════════════════════════════════════════
    df['Highest_Score_Overall'] = df.groupby(['user_id', 'admin_unit_name', 'module_name'])['Total Score'].transform('max')

    df = df.sort_values(['user_id', 'module_name', 'contest_date'])
    df['Attempt_number'] = df.groupby(['user_id', 'module_name']).cumcount() + 1

    threshold = 64
    df['Status'] = df['Total Score'].apply(lambda x: 'Cleared' if x >= threshold else 'Not Cleared')

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 5: FINAL REPORT (au_start_date retained)
    # ═══════════════════════════════════════════════════════════════════════════
    final_report = df[[
        'user_id',
        'admin_unit_name',
        'au_start_date',
        'module_name',
        'Attempt_number',
        'contest_date',
        'Week_Start_Date',
        'Total Score',
        'Highest_Score_Overall',
        'Status'
    ]].copy()

    final_report['contest_date'] = final_report['contest_date'].dt.strftime('%Y-%m-%d')
    final_report['Week_Start_Date'] = final_report['Week_Start_Date'].dt.strftime('%Y-%m-%d')
    final_report['au_start_date'] = final_report['au_start_date'].dt.strftime('%Y-%m-%d')

    Mid_Module_Attempt_wise = final_report.rename(columns={
        'admin_unit_name': 'Admin_Unit_name',
        'contest_date': 'contest_date_Mid_MC',
        'Attempt_number': 'Attempt_no_Mid_MC',
        'Total Score': 'Total_Score_Mid_MC',
        'Highest_Score_Overall': 'Highest_Score_Mid_MC',
        'Status': 'Status_Mid_MC'
    })

    Mid_Module_Attempt_wise = Mid_Module_Attempt_wise[[
        'user_id', 'Admin_Unit_name', 'au_start_date', 'module_name', 'Attempt_no_Mid_MC',
        'contest_date_Mid_MC', 'Week_Start_Date', 'Total_Score_Mid_MC',
        'Highest_Score_Mid_MC', 'Status_Mid_MC'
    ]]

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 6: MERGE WITH MASTER DATA
    # ═══════════════════════════════════════════════════════════════════════════
    print("Merging with Master Data...")
    workbook = gc.open('DS Full program - All Intake 2026')
    worksheet1 = workbook.worksheet('Master Data 2023-2026')
    data1 = worksheet1.get_all_values()
    df_master = pd.DataFrame(data1)

    df_master.columns = df_master.iloc[0]
    df_master = df_master.iloc[1:].copy()
    df_master = df_master.rename(columns={'User ID ': 'user_id'})
    df_master = df_master[~df_master['Persona'].isin(['NF', '#N/A']) & df_master['Persona'].notna()].copy()
    df_master['user_id'] = clean_to_int(df_master['user_id'])

    Mid_Module_Attempt_wise['user_id'] = clean_to_int(Mid_Module_Attempt_wise['user_id'])

    df_mid_MC = pd.merge(Mid_Module_Attempt_wise, df_master, on='user_id', how='left')
    print(f"✓ Merged data: {len(df_mid_MC)} rows")

    # merge batch-level stats onto the student rows (constant per batch)
    df_mid_MC = pd.merge(df_mid_MC, df_bs, on='Admin_Unit_name', how='left')
    print(f"✓ After batch-stats merge: {len(df_mid_MC)} rows")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 7: WRITE STUDENT-LEVEL TAB
    # ═══════════════════════════════════════════════════════════════════════════
    sheet = gc.open_by_key('1I4HAAkbZl2Zr6IblLRh1AasfBj1LbZCj0bQ-X1wLGFM')
    worksheet = sheet.worksheet("Student-level-Mid-MC")
    worksheet.clear()
    set_with_dataframe(worksheet, df_mid_MC, include_index=False, include_column_header=True)
    print(f"✓ Wrote Student-level-Mid-MC: {len(df_mid_MC)} rows")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 10: HEAD-TO-HEAD STUDENT-WISE (Mid-MC)  → Looker
    # One row per student-module. Rolls up exactly into the batch agg.
    # ═══════════════════════════════════════════════════════════════════════════
    THRESHOLD = 64
    key = ['user_id', 'Admin_Unit_name', 'module_name']

    aw = Mid_Module_Attempt_wise.copy()
    aw['Total_Score_Mid_MC'] = pd.to_numeric(aw['Total_Score_Mid_MC'], errors='coerce')

    # au_start_date is per student-module — grab it once so A1-only rows keep a Batch
    batch_ref = (aw.assign(_d=pd.to_datetime(aw['au_start_date'], errors='coerce'))
                 .sort_values('_d').drop_duplicates(key)[key + ['au_start_date']])

    a1 = (aw[aw['Attempt_no_Mid_MC'] == 1]
          .rename(columns={'contest_date_Mid_MC': 'A1_date',
                           'Total_Score_Mid_MC':  'A1_Score',
                           'Status_Mid_MC':       'A1_Status'})
          [key + ['A1_date', 'A1_Score', 'A1_Status']].drop_duplicates(key))

    a2 = (aw[aw['Attempt_no_Mid_MC'] == 2]
          .rename(columns={'contest_date_Mid_MC': 'A2_date',
                           'Total_Score_Mid_MC':  'A2_Score',
                           'Status_Mid_MC':       'A2_Status'})
          [key + ['A2_date', 'Week_Start_Date', 'A2_Score', 'A2_Status']].drop_duplicates(key))

    # outer = all who sat A1 OR A2
    view = a1.merge(a2, on=key, how='outer').merge(batch_ref, on=key, how='left')

    # ---- per-student flags -------------------------------------------------------
    view['Took_A1']     = view['A1_Score'].notna().map({True: 'Yes', False: 'No'})
    view['Took_A2']     = view['A2_Score'].notna().map({True: 'Yes', False: 'No'})
    view['Cleared_A1']  = view['A1_Status'].map({'Cleared': 'Yes', 'Not Cleared': 'No'}).fillna('')
    view['Cleared_A2']  = view['A2_Status'].map({'Cleared': 'Yes', 'Not Cleared': 'No'}).fillna('')
    view['Recovered_A1fail_A2clear'] = (
        (view['A1_Status'] == 'Not Cleared') & (view['A2_Status'] == 'Cleared')
    ).map({True: 'Yes', False: 'No'})

    # Improved / delta only defined when both sittings exist
    both = view['A1_Score'].notna() & view['A2_Score'].notna()
    view['Improved_vs_A1'] = ''
    view.loc[both, 'Improved_vs_A1'] = (view.loc[both, 'A2_Score'] > view.loc[both, 'A1_Score']).map({True: 'Yes', False: 'No'})
    view['Score_Delta'] = (view['A2_Score'] - view['A1_Score']).where(both)

    # ---- Batch + date formatting -------------------------------------------------
    d = pd.to_datetime(view['au_start_date'], errors='coerce')
    view['Batch']       = d.dt.to_period('M').dt.to_timestamp()
    view['Batch_Label'] = d.dt.strftime('%b %Y')
    view = view.sort_values(['Batch', 'user_id'])
    for c in ['A1_date', 'A2_date', 'Week_Start_Date', 'au_start_date', 'Batch']:
        view[c] = pd.to_datetime(view[c], errors='coerce').dt.strftime('%Y-%m-%d')

    # ---- master merge (suffixes avoid Batch/other collisions) --------------------
    view['user_id'] = clean_to_int(view['user_id'])
    view = view.merge(df_master, on='user_id', how='left', suffixes=('', '_master'))
    view = view.drop(columns=[c for c in view.columns if c.endswith('_master')], errors='ignore')

    # ---- column order ------------------------------------------------------------
    front = ['user_id', 'Admin_Unit_name', 'module_name', 'au_start_date',
             'Batch', 'Batch_Label',
             'Took_A1', 'A1_date', 'A1_Score', 'A1_Status', 'Cleared_A1',
             'Took_A2', 'A2_date', 'A2_Score', 'A2_Status', 'Cleared_A2',
             'Recovered_A1fail_A2clear', 'Improved_vs_A1', 'Score_Delta']
    view = view[front + [c for c in view.columns if c not in front]]

    print(f"Mid-MC student-wise head-to-head: {len(view)} rows")
    print(f"  Took A1: {(view['Took_A1']=='Yes').sum()} | Took A2: {(view['Took_A2']=='Yes').sum()} | "
          f"Recovered: {(view['Recovered_A1fail_A2clear']=='Yes').sum()}")

    # ---- write -------------------------------------------------------------------
    out_tab = "MidMC-HeadToHead-StudentWise"
    try:
        ws = sheet.worksheet(out_tab)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=out_tab, rows=2, cols=2)
    ws.clear()
    set_with_dataframe(ws, view, include_index=False, include_column_header=True)

    print("\n" + "=" * 60)
    print("✅ UPLOAD SUCCESSFUL")
    print(f"Rows: {len(view)} | Columns: {len(view.columns)} → '{out_tab}'")
    print("=" * 60)

except Exception as e:
    print(f"❌ Pipeline failed: {e}")
    traceback.print_exc()
    sys.exit(1)

mins, secs = divmod(time.time() - start_time, 60)
print(f"\n🎯 Module Contest & Mid Module Contest Pipeline completed successfully in {int(mins)}m {int(secs)}s")
sys.exit(0)
