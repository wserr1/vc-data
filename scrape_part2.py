import pandas as pd
import requests
import os
from dotenv import load_dotenv
load_dotenv()

AIRTABLE_API_KEY = os.getenv("AIRTABLE_TOKEN")
BASE_ID = "appyfDILW0PkDwiHH"
TABLE_NAME = "VC Firms"
HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

print("Loading fund data...")

funds = pd.read_csv("ERA_Schedule_D_7B1_20111105_20241231.csv", encoding="latin1", low_memory=False)
era = pd.read_csv("ERA_ADV_Base_20111105_20241231.csv", encoding="latin1", low_memory=False)

era["DateSubmitted"] = pd.to_datetime(era["DateSubmitted"])
era_latest = era.sort_values("DateSubmitted").groupby("1A").last().reset_index()

merged = era_latest.merge(funds, on="FilingID", how="inner")

# Group fund names per firm
fund_names = merged.groupby("1A")["Fund Name"].apply(
    lambda x: ", ".join(x.dropna().unique()[:5])
).reset_index()
fund_names.columns = ["Firm", "Fund Names"]

# Get minimum investment — take the most common non-zero value per firm
def get_min_investment(series):
    vals = series.dropna()
    vals = vals[vals > 0]
    if len(vals) == 0:
        return None
    return int(vals.median())

min_investments = merged.groupby("1A")["Minimum Investment"].apply(get_min_investment).reset_index()
min_investments.columns = ["Firm", "Min Investment"]

# Get fund types
fund_types = merged.groupby("1A")["Fund Type"].apply(
    lambda x: ", ".join(x.dropna().unique()[:3])
).reset_index()
fund_types.columns = ["Firm", "Fund Types"]

# Merge all
firm_fund_data = fund_names.merge(min_investments, on="Firm").merge(fund_types, on="Firm")

print(f"Found fund data for {len(firm_fund_data)} firms")
print("\nSample:")
for _, row in firm_fund_data.head(5).iterrows():
    print(f"{row['Firm']}: {row['Fund Names'][:60]} | Min: ${row['Min Investment']:,}" if row['Min Investment'] else f"{row['Firm']}: {row['Fund Names'][:60]}")

# Get Airtable record map
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

print(f"Found {len(record_map)} firms in Airtable")

firm_fund_data = firm_fund_data[firm_fund_data["Firm"].isin(record_map.keys())]
print(f"Updating {len(firm_fund_data)} firms with fund data...")

updated = 0
for _, row in firm_fund_data.iterrows():
    fields = {
        "Fund Number": row["Fund Types"] if pd.notna(row["Fund Types"]) else "",
    }
    if pd.notna(row["Min Investment"]) and row["Min Investment"]:
        fields["Check Size Min"] = float(row["Min Investment"])
    try:
        resp = requests.patch(
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}/{record_map[row['Firm']]}",
            headers=HEADERS,
            json={"fields": fields}
        )
        if resp.status_code == 200:
            updated += 1
            if updated % 100 == 0:
                print(f"Updated {updated} firms so far...")
    except:
        pass
    import time
    time.sleep(0.1)

print(f"\nDone! Updated {updated} firms with fund data.")