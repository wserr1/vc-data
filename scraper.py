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
vc_exclude = ["REAL ESTATE", "ENERGY", "INFRASTRUCTURE", "CREDIT", "DEBT", "LENDING", "MORTGAGE", "INSURANCE", "WEALTH", "ADVISORY", "FINANCIAL SERVICES"]

include_pattern = "|".join(vc_include)
exclude_pattern = "|".join(vc_exclude)

vc_firms = latest[
    latest["1A"].str.contains(include_pattern, na=False, case=False) &
    ~latest["1A"].str.contains(exclude_pattern, na=False, case=False) &
    latest["5F2a"].notna() &
    (latest["5F2a"] > 50_000_000)
].copy()

vc_firms = vc_firms.sort_values("5F2a", ascending=False)

headers = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

# Get ALL existing firms from Airtable (handles pagination)
print("Checking existing records...")
existing_firms = set()
offset = None
while True:
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}"
    if offset:
        url += f"?offset={offset}"
    resp = requests.get(url, headers=headers)
    data = resp.json()
    for record in data.get("records", []):
        existing_firms.add(record["fields"].get("Firm", ""))
    offset = data.get("offset")
    if not offset:
        break

print(f"Found {len(existing_firms)} existing firms")
print(f"Pushing {len(vc_firms)} firms to Airtable...")

# Get record IDs so we can update existing records
print("Getting record IDs...")
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
        record_map[name] = record["id"]
    offset = data.get("offset")
    if not offset:
        break

print(f"Found {len(record_map)} existing records")

success = 0
added = 0
for _, row in vc_firms.iterrows():
    fields = {
        "Firm": row["1A"],
        "City": row["1F1-City"] if pd.notna(row["1F1-City"]) else "",
        "State": row["1F1-State"] if pd.notna(row["1F1-State"]) else "",
        "AUM": float(row["5F2a"]),
        "Last Filed": row["DateSubmitted"].strftime("%B %Y"),
        "Phone": str(row["1F3"]) if pd.notna(row["1F3"]) else "",
        "Address": f"{row['1F1-Street 1']} {row['1F1-Street 2']}".strip() if pd.notna(row["1F1-Street 1"]) else "",
        "Employees": int(float(row["5A"])) if pd.notna(row["5A"]) and str(row["5A"]).replace('.','').isdigit() else 0,
        "Clients": int(float(row["5B1"])) if pd.notna(row["5B1"]) and str(row["5B1"]).replace('.','').isdigit() else 0,
        "CRD": str(row["1E1"]) if pd.notna(row["1E1"]) else ""
    }

    if row["1A"] in record_map:
        response = requests.patch(
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}/{record_map[row['1A']]}",
            headers=headers,
            json={"fields": fields}
        )
        if response.status_code == 200:
            success += 1
        else:
            print(f"Error updating {row['1A']}: {response.text[:50]}")
    else:
        response = requests.post(
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}",
            headers=headers,
            json={"fields": fields}
        )
        if response.status_code == 200:
            added += 1
            print(f"Added: {row['1A']}")
        else:
            print(f"Error adding {row['1A']}: {response.text[:50]}")

print(f"\nDone! Updated {success} firms, added {added} new firms.")

# Add famous exempt reporting firms
exempt_firms = [
    {"Firm": "KLEINER PERKINS", "City": "MENLO PARK", "State": "CA", "Notes": "Exempt Reporting Adviser"},
    {"Firm": "GREYLOCK PARTNERS", "City": "MENLO PARK", "State": "CA", "Notes": "Exempt Reporting Adviser"},
    {"Firm": "BENCHMARK CAPITAL", "City": "SAN FRANCISCO", "State": "CA", "Notes": "Exempt Reporting Adviser"},
    {"Firm": "ACCEL PARTNERS", "City": "PALO ALTO", "State": "CA", "Notes": "Exempt Reporting Adviser"},
    {"Firm": "FOUNDERS FUND", "City": "SAN FRANCISCO", "State": "CA", "Notes": "Exempt Reporting Adviser"},
    {"Firm": "LIGHTSPEED VENTURE PARTNERS", "City": "MENLO PARK", "State": "CA", "Notes": "Exempt Reporting Adviser"},
    {"Firm": "UNION SQUARE VENTURES", "City": "NEW YORK", "State": "NY", "Notes": "Exempt Reporting Adviser"},
    {"Firm": "FIRST ROUND CAPITAL", "City": "SAN FRANCISCO", "State": "CA", "Notes": "Exempt Reporting Adviser"},
    {"Firm": "ANDREESSEN HOROWITZ", "City": "MENLO PARK", "State": "CA", "Notes": "Exempt Reporting Adviser"},
]

print("\nAdding exempt reporting firms...")
for firm in exempt_firms:
    if firm["Firm"] in existing_firms:
        print(f"Skipped (already exists): {firm['Firm']}")
        continue
    data = {
        "fields": {
            "Firm": firm["Firm"],
            "City": firm["City"],
            "State": firm["State"],
            "Notes": firm["Notes"]
        }
    }
    response = requests.post(
        f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}",
        headers=headers,
        json=data
    )
    if response.status_code == 200:
        print(f"Added: {firm['Firm']}")
    else:
        print(f"Error: {firm['Firm']} — {response.text[:50]}")
    