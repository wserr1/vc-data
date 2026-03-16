import pandas as pd
import warnings
import requests
warnings.filterwarnings("ignore")

# ---- YOUR DETAILS (fill these in) ---- #
AIRTABLE_API_KEY = "pat7pJMSiwMneUQHQ.7bd57c35f1b4d03431d740fd52eee7af31ef304992d79e8df9a36570c8caa89d"
BASE_ID = "appyfDILW0PkDwiHH"
TABLE_NAME = "VC Firms"
# --------------------------------------- #

print("Loading SEC data... (may take 30 seconds)")

df = pd.read_csv("IA_ADV_Base_A_20111105_20241231.csv", encoding="latin1", low_memory=False)

df["DateSubmitted"] = pd.to_datetime(df["DateSubmitted"])
latest = df.sort_values("DateSubmitted").groupby("1A").last().reset_index()

confirmed_firms = [
    "SEQUOIA CAPITAL OPERATIONS, LLC",
    "AH CAPITAL MANAGEMENT, L.L.C.",
    "TIGER GLOBAL MANAGEMENT, LLC",
    "GENERAL CATALYST GROUP MANAGEMENT, LLC",
    "INSIGHT VENTURE MANAGEMENT, LLC",
    "PANTHEON VENTURES (US) LP",
    "SAPPHIRE VENTURES, LLC",
    "BAIN CAPITAL VENTURES, LP",
    "ALTOS VENTURES MANAGEMENT, INC.",
    "INDUSTRY VENTURES, L.L.C."
]

results = latest[latest["1A"].isin(confirmed_firms)].copy()
results = results.sort_values("5F2a", ascending=False)

print("Pushing to Airtable...")

headers = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

for _, row in results.iterrows():
    data = {
        "fields": {
            "Firm": row["1A"],
            "City": row["1F1-City"],
            "State": row["1F1-State"],
            "AUM": float(row["5F2a"]),
            "Last Filed": row["DateSubmitted"].strftime("%B %Y")
        }
    }
    response = requests.post(
        f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}",
        headers=headers,
        json=data
    )
    if response.status_code == 200:
        print(f"Added: {row['1A']}")
    else:
        print(f"Error with {row['1A']}: {response.text}")

print("Done!")