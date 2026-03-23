import pandas as pd
import requests
import os
import time
from dotenv import load_dotenv
load_dotenv()

AIRTABLE_API_KEY = os.getenv("AIRTABLE_TOKEN")
BASE_ID = "appyfDILW0PkDwiHH"
TABLE_NAME = "VC Firms"
HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

era = pd.read_csv("ERA_ADV_Base_20111105_20241231.csv", encoding="latin1", low_memory=False)
era["DateSubmitted"] = pd.to_datetime(era["DateSubmitted"])
era_latest = era.sort_values("DateSubmitted").groupby("1A").last().reset_index()

print("Loading parent company data...")

df = pd.read_csv("ERA_Schedule_D_10B_20111105_20241231.csv", encoding="latin1", low_memory=False, on_bad_lines='skip')
merged = era_latest.merge(df, on="FilingID", how="inner")

firm_parents = merged.groupby("1A")["Full Legal Name"].apply(
    lambda x: ", ".join(x.dropna().unique()[:3])
).reset_index()
firm_parents.columns = ["Firm", "LP Base"]

print(f"Found parent data for {len(firm_parents)} firms")
print("\nSample:")
for _, row in firm_parents.head(5).iterrows():
    print(f"{row['Firm']}: {row['LP Base']}")

print("\nGetting Airtable records...")
record_map = {}
offset = None
while True:
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}"
    if offset:
        url += f"?offset={offset}"
    resp = requests.get(url, headers=HEADERS)
    data = resp.json()
    for record in data.get("records", []):
        name = record["fields"].get("Firm", "")
        record_map[name] = record["id"]
    offset = data.get("offset")
    if not offset:
        break

firm_parents = firm_parents[firm_parents["Firm"].isin(record_map.keys())]
print(f"Updating {len(firm_parents)} firms...")

updated = 0
for _, row in firm_parents.iterrows():
    try:
        resp = requests.patch(
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}/{record_map[row['Firm']]}",
            headers=HEADERS,
            json={"fields": {"LP Base": row["LP Base"]}}
        )
        if resp.status_code == 200:
            updated += 1
    except:
        pass
    time.sleep(0.05)

print(f"\nDone! Updated {updated} firms with parent company data.")