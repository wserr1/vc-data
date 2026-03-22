import pandas as pd
import requests
import warnings
warnings.filterwarnings("ignore")
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

print("Token:", AIRTABLE_API_KEY[:10] if AIRTABLE_API_KEY else "NONE")

print("Loading website data...")

ia_sites = pd.read_csv("IA_Schedule_D_1I_20111105_20241231.csv", encoding="latin1", low_memory=False)
era_sites = pd.read_csv("ERA_Schedule_D_7B1A28_websites_20111105_20241231.csv", encoding="latin1", low_memory=False)
era = pd.read_csv("ERA_ADV_Base_20111105_20241231.csv", encoding="latin1", low_memory=False)
adv = pd.read_csv("IA_ADV_Base_A_20111105_20241231.csv", encoding="latin1", low_memory=False)

era["DateSubmitted"] = pd.to_datetime(era["DateSubmitted"])
era_latest = era.sort_values("DateSubmitted").groupby("1A").last().reset_index()

adv["DateSubmitted"] = pd.to_datetime(adv["DateSubmitted"])
adv_latest = adv.sort_values("DateSubmitted").groupby("1A").last().reset_index()

# Get Airtable record map
print("Getting Airtable records...")
record_map = {}
offset = None
while True:
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}"
    if offset:
        url += f"?offset={offset}"
    resp = requests.get(url, headers=HEADERS)
    data = resp.json()
    print("Status:", resp.status_code, "Records:", len(data.get("records", [])))
    for record in data.get("records", []):
        name = record["fields"].get("Firm", "")
        record_map[name] = record["id"]
    offset = data.get("offset")
    if not offset:
        break

print(f"Found {len(record_map)} firms in Airtable")

# Combine IA and ERA websites
bad_domains = ["vimeo", "linkedin", "twitter", "facebook", "youtube", "angel.co",
               "sec.gov", "intralinks", "fundfire", "bloomberg", "wsj.com",
               "reuters", "crunchbase", "pitchbook", "instagram", "hedge",
               "spotify", "soundcloud", "slideshare", "dropbox"]

ia_merged = adv_latest.merge(ia_sites, on="FilingID", how="inner")
ia_merged = ia_merged[~ia_merged["Website"].str.lower().str.contains("|".join(bad_domains), na=False)]
ia_merged = ia_merged.sort_values("Website")
ia_firm_sites = ia_merged.groupby("1A")["Website"].first().reset_index()
ia_firm_sites.columns = ["Firm", "Website"]

era_merged = era_latest.merge(era_sites, on="FilingID", how="inner")
era_merged = era_merged[~era_merged["Website Address"].str.lower().str.contains("|".join(bad_domains), na=False)]
era_merged = era_merged.sort_values("Website Address")
era_firm_sites = era_merged.groupby("1A")["Website Address"].first().reset_index()
era_firm_sites.columns = ["Firm", "Website"]

all_sites = pd.concat([ia_firm_sites, era_firm_sites]).drop_duplicates(subset=["Firm"])
all_sites = all_sites[all_sites["Firm"].isin(record_map.keys())]

print(f"Updating {len(all_sites)} firms with websites...")

updated = 0
for _, row in all_sites.iterrows():
    try:
        response = requests.patch(
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}/{record_map[row['Firm']]}",
            headers=HEADERS,
            json={"fields": {"Website": row["Website"]}}
        )
        if response.status_code == 200:
            updated += 1
            if updated % 100 == 0:
                print(f"Updated {updated} firms so far...")
    except:
        pass

print(f"\nDone! Updated {updated} firms with website data.")