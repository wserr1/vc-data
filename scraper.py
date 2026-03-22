import pandas as pd
import warnings
import requests
warnings.filterwarnings("ignore")

# ---- YOUR DETAILS (fill these in) ---- #
import os
from dotenv import load_dotenv
load_dotenv()
AIRTABLE_API_KEY = os.getenv("AIRTABLE_TOKEN")
BASE_ID = "appyfDILW0PkDwiHH"
TABLE_NAME = "VC Firms"
# --------------------------------------- #

print("Loading SEC data... (may take 30 seconds)")

df = pd.read_csv("IA_ADV_Base_A_20111105_20241231.csv", encoding="latin1", low_memory=False)
df["DateSubmitted"] = pd.to_datetime(df["DateSubmitted"])
latest = df.sort_values("DateSubmitted").groupby("1A").last().reset_index()

vc_include = ["VENTURE", "VENTURES"]
vc_exclude = ["REAL ESTATE", "ENERGY", "INFRASTRUCTURE", "CREDIT", "DEBT",
              "LENDING", "MORTGAGE", "INSURANCE", "WEALTH", "ADVISORY",
              "FINANCIAL SERVICES"]

vc_firms = latest[
    latest["1A"].str.contains("|".join(vc_include), na=False, case=False) &
    ~latest["1A"].str.contains("|".join(vc_exclude), na=False, case=False) &
    latest["5F2a"].notna() &
    (latest["5F2a"] > 50_000_000)
].copy().sort_values("5F2a", ascending=False)

headers = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

print("Getting Airtable records...")
existing_firms = set()
record_map = {}
offset = None
while True:
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}"
    if offset:
        url += f"?offset={offset}"
    resp = requests.get(url, headers=headers)
    data = resp.json()
    for record in data.get("records", []):
        name = record["fields"].get("Firm", "")
        existing_firms.add(name)
        record_map[name] = record["id"]
    offset = data.get("offset")
    if not offset:
        break

print(f"Found {len(existing_firms)} existing firms")

def safe_str(val):
    return str(val) if pd.notna(val) else ""

def safe_int(val):
    try:
        return int(float(val)) if pd.notna(val) else 0
    except:
        return 0

def safe_float(val):
    try:
        return float(val) if pd.notna(val) else 0.0
    except:
        return 0.0

print(f"Updating {len(vc_firms)} ADV firms...")
success = 0
added = 0
for _, row in vc_firms.iterrows():
    comp_types = []
    if row.get("5E1") == "Y": comp_types.append("% of AUM")
    if row.get("5E2") == "Y": comp_types.append("Hourly")
    if row.get("5E4") == "Y": comp_types.append("Fixed fees")
    if row.get("5E6") == "Y": comp_types.append("Performance-based")
    if row.get("5E7") == "Y":
        other = safe_str(row.get("5E7-Other", ""))
        comp_types.append(other if other else "Other")

    fields = {
        "Firm": row["1A"],
        "City": safe_str(row["1F1-City"]),
        "State": safe_str(row["1F1-State"]),
        "AUM": safe_float(row["5F2a"]),
        "Last Filed": row["DateSubmitted"].strftime("%B %Y"),
        "Phone": safe_str(row["1F3"]),
        "Address": f"{safe_str(row['1F1-Street 1'])} {safe_str(row['1F1-Street 2'])}".strip(),
        "Employees": safe_int(row["5A"]),
        "Clients": safe_int(row["5B1"]),
        "CRD": safe_str(row["1E1"]),
        "Business Hours": f"{safe_str(row['1F2-M-F'])} {safe_str(row['1F2-Hours'])}".strip(),
        "Fax": safe_str(row["1F4"]),
        "Pct Non-US Clients": safe_float(row["5C2"]),
        "Num Accounts": safe_int(row["5F2f"]),
        "Advisory Employees": safe_int(row["5B1"]),
        "Compensation": ", ".join(comp_types),
        "Non-US AUM": safe_float(row["5F3"]),
        "Private Fund Advisor": "Yes" if row.get("7B") == "Y" else "No",
        "Num Custodians": safe_int(row["9F"]),
        "Signatory": safe_str(row["Signatory"]),
        "Signatory Title": safe_str(row["Title"]),
        "Discretionary AUM": safe_float(row["5F2a"]),
        "Non Discretionary AUM": safe_float(row["5F2b"])
    }

    if row["1A"] in record_map:
        resp = requests.patch(
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}/{record_map[row['1A']]}",
            headers=headers, json={"fields": fields}
        )
        if resp.status_code == 200:
            success += 1
    else:
        resp = requests.post(
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}",
            headers=headers, json={"fields": fields}
        )
        if resp.status_code == 200:
            added += 1

print(f"ADV Done! Updated {success}, added {added}")

print("\nLoading ERA data...")
era = pd.read_csv("ERA_ADV_Base_20111105_20241231.csv", encoding="latin1", low_memory=False)
era["DateSubmitted"] = pd.to_datetime(era["DateSubmitted"])
era_latest = era.sort_values("DateSubmitted").groupby("1A").last().reset_index()
era_us = era_latest[era_latest["1F1-Country"] == "United States"].copy()

era_added = 0
era_skipped = 0
for _, row in era_us.iterrows():
    if row["1A"] in existing_firms:
        era_skipped += 1
        continue
    fields = {
        "Firm": row["1A"],
        "City": safe_str(row["1F1-City"]),
        "State": safe_str(row["1F1-State"]),
        "Address": f"{safe_str(row['1F1-Street 1'])} {safe_str(row['1F1-Street 2'])}".strip(),
        "Phone": safe_str(row["1F3"]),
        "Fax": safe_str(row["1F4"]),
        "Business Hours": f"{safe_str(row['1F2-M-F'])} {safe_str(row['1F2-Hours'])}".strip(),
        "CRD": safe_str(row["1E1"]),
        "Last Filed": row["DateSubmitted"].strftime("%B %Y"),
        "Notes": "Exempt Reporting Adviser",
        "Signatory": safe_str(row["Signatory"]),
        "Signatory Title": safe_str(row["Title"])
    }
    resp = requests.post(
        f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}",
        headers=headers, json={"fields": fields}
    )
    if resp.status_code == 200:
        era_added += 1
        if era_added % 100 == 0:
            print(f"Added {era_added} ERA firms...")

print(f"ERA Done! Added {era_added}, skipped {era_skipped}")