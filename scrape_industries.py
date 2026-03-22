import requests
import warnings
warnings.filterwarnings("ignore")
from bs4 import BeautifulSoup
import time
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

print("Loading firms with websites from Airtable...")

all_records = []
offset = None
while True:
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}"
    if offset:
        url += f"?offset={offset}"
    resp = requests.get(url, headers=HEADERS)
    data = resp.json()
    for record in data.get("records", []):
        if record["fields"].get("Website") and not record["fields"].get("Industry Focus"):
            all_records.append(record)
    offset = data.get("offset")
    if not offset:
        break

print(f"Found {len(all_records)} firms to scrape")

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

scraped = 0
failed = 0
for record in all_records:
    website = record["fields"]["Website"]
    result = scrape_firm_website(website)
    if result and (result["Industry Focus"] or result["Stage Focus"]):
        try:
            requests.patch(
                f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}/{record['id']}",
                headers=HEADERS,
                json={"fields": result}
            )
            scraped += 1
        except:
            failed += 1
    else:
        failed += 1
    if (scraped + failed) % 50 == 0:
        print(f"Progress: {scraped + failed}/{len(all_records)} — {scraped} successful")
    time.sleep(0.5)

print(f"\nDone! Scraped {scraped} firms, {failed} failed or empty.")