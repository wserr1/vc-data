import os
from dotenv import load_dotenv
load_dotenv()
print("ENV path:", os.path.abspath(".env"))
print("Token from env:", os.getenv("AIRTABLE_TOKEN", "NOT FOUND")[:10])

AIRTABLE_API_KEY = os.getenv("AIRTABLE_TOKEN")
BASE_ID = "appyfDILW0PkDwiHW"
TABLE_NAME = "VC Firms"

HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}