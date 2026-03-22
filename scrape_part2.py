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

print("Loading office locations data...")

era_offices = pd.read_csv("ERA_Schedule_D_1F_20111105_20241231.csv", encoding="latin1", low_memory=False, on_bad_lines='skip')
ia_offices = pd.read_csv("IA_Schedule_D_1F_20111105_20241231.csv", encoding="latin1", low_memory=False, on_bad_lines='skip')
era = pd.read_csv("ERA_ADV_Base_20111105_20241231.csv", encoding="latin1", low_memory=False)
adv = pd.read_csv("IA_ADV_Base_A_20111105_20241231.csv", encoding="latin1", low_memory=False)

era["DateSubmitted"] = pd.to_datetime(era["DateSubmitted"])
era_latest = era.sort_values("DateSubmitted").groupby("1A").last().reset_index()

adv["DateSubmitted"] = pd.to_datetime(adv["DateSubmitted"])
adv_latest = adv.sort_values("DateSubmitted").groupby("1A").last().reset_index()

era_merged = era_latest.merge(era_offices, on="FilingID", how="inner")
ia_merged = adv_latest.merge(ia_offices, on="FilingID", how="inner")

def get_locations(df):
    us = df[df["Country"] == "United States"]
    states = us.groupby("1A")["State"].apply(
        lambda x: ", ".join(x.dropna().unique())
    ).reset_index()
    states.columns = ["Firm", "Office States"]
    
    intl = df[df["Country"] != "United States"]
    countries = intl.groupby("1A")["Country"].apply(
        lambda x: ", ".join(x.dropna().unique()[:5])
    ).reset_index()
    countries.columns = ["Firm", "Intl Offices"]
    
    return states.merge(countries, on="Firm", how="outer")

era_locs = get_locations(era_merged)
ia_locs = get_locations(ia_merged)
all_locs = pd.concat([era_locs, ia_locs]).drop_duplicates(subset=["Firm"])

print(f"Found location data for {len(all_locs)} firms")

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

all_locs = all_locs[all_locs["Firm"].isin(record_map.keys())]
print(f"Updating {len(all_locs)} firms with location data...")

import time
updated = 0
for _, row in all_locs.iterrows():
    fields = {}
    if pd.notna(row.get("Office States")) and row["Office States"]:
        fields["Geographic Focus"] = row["Office States"]
    if pd.notna(row.get("Intl Offices")) and row["Intl Offices"]:
        existing = fields.get("Geographic Focus", "")
        fields["Geographic Focus"] = f"{existing}, {row['Intl Offices']}".strip(", ")
    
    if not fields:
        continue
    
    try:
        resp = requests.patch(
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}/{record_map[row['Firm']]}",
            headers=HEADERS,
            json={"fields": fields}
        )
        if resp.status_code == 200:
            updated += 1
            if updated % 200 == 0:
                print(f"Updated {updated} firms so far...")
    except:
        pass
    time.sleep(0.05)

print(f"\nDone! Updated {updated} firms with location data.")