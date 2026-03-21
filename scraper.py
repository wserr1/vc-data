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

# Get ALL existing firms from Airtable
print("Checking existing records...")
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

print(f"Pushing {len(vc_firms)} firms to Airtable...")
print("Getting record IDs...")

success = 0
added = 0
for _, row in vc_firms.iterrows():
    comp_types = []
    if row.get("5E1") == "Y": comp_types.append("% of AUM")
    if row.get("5E2") == "Y": comp_types.append("Hourly")
    if row.get("5E3") == "Y": comp_types.append("Subscription")
    if row.get("5E4") == "Y": comp_types.append("Fixed fees")
    if row.get("5E5") == "Y": comp_types.append("Commissions")
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
        response = requests.patch(
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}/{record_map[row['1A']]}",
            headers=headers,
            json={"fields": fields}
        )
        if response.status_code == 200:
            success += 1
        else:
            print(f"Error updating {row['1A']}: {response.text[:200]}")
    else:
        response = requests.post(
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}",
            headers=headers,
            json={"fields": fields}
        )
        if response.status_code == 200:
            added += 1
        else:
            print(f"Error adding {row['1A']}: {response.text[:200]}")

print(f"\nDone! Updated {success} firms, added {added} new firms.")

# ============================================
# PART 2: Add ERA exempt reporting VC firms
# ============================================
print("\nLoading ERA data...")

era = pd.read_csv("ERA_ADV_Base_20111105_20241231.csv", encoding="latin1", low_memory=False)
era["DateSubmitted"] = pd.to_datetime(era["DateSubmitted"])
era_latest = era.sort_values("DateSubmitted").groupby("1A").last().reset_index()

era_us = era_latest[
    era_latest["1F1-Country"] == "United States"
].copy()

print(f"Total US ERA firms: {len(era_us)}")
print("Adding to Airtable...")

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

    response = requests.post(
        f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}",
        headers=headers,
        json={"fields": fields}
    )

    if response.status_code == 200:
        era_added += 1
        if era_added % 50 == 0:
            print(f"Added {era_added} ERA firms so far...")
    else:
        pass

print(f"\nERA Done! Added {era_added} new firms, skipped {era_skipped} existing.")

print("\nLoading website data...")

ia_sites = pd.read_csv("IA_Schedule_D_1I_20111105_20241231.csv", encoding="latin1", low_memory=False)

ia_with_names = latest.merge(ia_sites, on="FilingID", how="inner")
ia_with_names = ia_with_names.sort_values("Website")
firm_websites = ia_with_names.groupby("1A")["Website"].first().reset_index()
firm_websites.columns = ["Firm", "Website"]
firm_websites = firm_websites[firm_websites["Firm"].isin(record_map.keys())]
print(f"Filtered to {len(firm_websites)} firms in our database")

print(f"Firms with websites: {len(firm_websites)}")

website_added = 0
for _, row in firm_websites.iterrows():
    if row["Firm"] in record_map:
        response = requests.patch(
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}/{record_map[row['Firm']]}",
            headers=headers,
            json={"fields": {"Website": row["Website"]}}
        )
        if response.status_code == 200:
            website_added += 1

print(f"Updated {website_added} firms with website data.")

# ============================================
# PART 4: Push ERA website data to Airtable
# ============================================
print("\nLoading ERA website data...")

era_sites = pd.read_csv("ERA_Schedule_D_7B1A28_websites_20111105_20241231.csv", encoding="latin1", low_memory=False)

era_with_names = era_latest.merge(era_sites, on="FilingID", how="inner")
era_with_names = era_with_names.sort_values("Website Address")
era_firm_websites = era_with_names.groupby("1A")["Website Address"].first().reset_index()
era_firm_websites.columns = ["Firm", "Website"]

era_firm_websites = era_firm_websites[era_firm_websites["Firm"].isin(record_map.keys())]
print(f"ERA firms with websites: {len(era_firm_websites)}")

era_web_added = 0
for _, row in era_firm_websites.iterrows():
    if row["Firm"] in record_map:
        response = requests.patch(
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}/{record_map[row['Firm']]}",
            headers=headers,
            json={"fields": {"Website": row["Website"]}}
        )
        if response.status_code == 200:
            era_web_added += 1
            if era_web_added % 100 == 0:
                print(f"Updated {era_web_added} ERA firms so far...")

print(f"Updated {era_web_added} ERA firms with website data.")

# ============================================
# PART 5: Scrape firm websites for industry/stage data
# ============================================
from bs4 import BeautifulSoup
import time

print("\nScraping firm websites for industry/stage data...")

def scrape_firm_website(url):
    try:
        if not url.lower().startswith("http"):
            url = "https://" + url
        response = requests.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        })
        soup = BeautifulSoup(response.text, "html.parser")
        for tag in soup(["script", "style", "nav", "footer"]):
            tag.decompose()
        text = soup.get_text(separator=" ").lower()
        text = " ".join(text.split())

        industry_keywords = {
            "AI / Machine Learning": ["artificial intelligence", "machine learning", "deep learning", "ai ", " ai,"],
            "SaaS / Software": ["saas", "software", "cloud", "enterprise software"],
            "Fintech": ["fintech", "financial technology", "payments", "banking"],
            "Healthcare / Biotech": ["healthcare", "biotech", "life sciences", "medical", "health"],
            "Consumer": ["consumer", "retail", "e-commerce", "marketplace"],
            "Deep Tech / Hardware": ["deep tech", "hardware", "semiconductor", "robotics"],
            "Climate / Clean Tech": ["climate", "clean tech", "cleantech", "energy", "sustainability"],
            "Cybersecurity": ["cybersecurity", "security", "cyber"],
            "EdTech": ["edtech", "education", "learning"],
            "Gaming": ["gaming", "games", "esports"],
        }

        stage_keywords = {
            "Pre-seed": ["pre-seed", "pre seed", "idea stage"],
            "Seed": ["seed stage", "seed investment", "seed fund", "early stage"],
            "Series A": ["series a", "series-a"],
            "Series B": ["series b", "series-b", "growth stage"],
            "Series C+": ["series c", "late stage", "growth equity"],
        }

        industries = [i for i, kws in industry_keywords.items() if any(kw in text for kw in kws)]
        stages = [s for s, kws in stage_keywords.items() if any(kw in text for kw in kws)]

        return {
            "Industry Focus": ", ".join(industries[:5]) if industries else "",
            "Stage Focus": ", ".join(stages) if stages else ""
        }
    except:
        return None

# Get all firms with websites from Airtable
all_records = []
offset = None
while True:
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}"
    if offset:
        url += f"?offset={offset}"
    resp = requests.get(url, headers=headers)
    data = resp.json()
    for record in data.get("records", []):
        if record["fields"].get("Website"):
            all_records.append(record)
    offset = data.get("offset")
    if not offset:
        break

print(f"Found {len(all_records)} firms with websites to scrape")

scraped = 0
failed = 0
for record in all_records:
    if record["fields"].get("Industry Focus"):
        continue
    website = record["fields"]["Website"]
    result = scrape_firm_website(website)
    if result and (result["Industry Focus"] or result["Stage Focus"]):
        requests.patch(
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}/{record['id']}",
            headers=headers,
            json={"fields": result}
        )
        scraped += 1
    else:
        failed += 1
    if (scraped + failed) % 50 == 0:
        print(f"Progress: {scraped + failed}/{len(all_records)} — {scraped} successful")
    time.sleep(0.5)

print(f"\nDone! Scraped {scraped} firms successfully, {failed} failed or empty.")