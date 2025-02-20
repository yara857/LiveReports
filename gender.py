import os
import json
import csv
import gspread
import pandas as pd
from datetime import datetime
from collections import defaultdict
from oauth2client.service_account import ServiceAccountCredentials
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from config import FACEBOOK_ACCESS_TOKEN

# Load Facebook Credentials from GitHub Secrets
# access_token = os.getenv("FB_ACCESS_TOKEN")
# if not access_token:
#     raise ValueError("Missing FACEBOOK_ACCESS_TOKEN secret.")

FacebookAdsApi.init(access_token=FACEBOOK_ACCESS_TOKEN)
me = User(fbid="me")
accounts = me.get_ad_accounts(fields=["id", "name"])

# Load Google Credentials from GitHub Secrets
# google_credentials_json = os.getenv("GOOGLE_CREDENTIALS")
# if not google_credentials_json:
#     raise ValueError("Missing GOOGLE_CREDENTIALS secret.")

# Save Google credentials to a temporary file
with open("striped-sunspot-451315-t6-8b0e56f96486.json", "w") as temp_file:
    json.dump(json.loads(google_credentials_json), temp_file)

# Authenticate with Google Sheets
scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("striped-sunspot-451315-t6-8b0e56f96486.json", scope)
client = gspread.authorize(creds)

# Open Google Sheet
spreadsheet = client.open("live spent with gender and age")
sheet = spreadsheet.sheet1  

# Get Date Range
today = datetime.today()
first_of_month = today.replace(day=1)
since_date = first_of_month.strftime("%Y-%m-%d")
until_date = today.strftime("%Y-%m-%d")
updated_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")  # Current UTC time

# Fetch Data
data = defaultdict(lambda: defaultdict(float))
gender_totals = defaultdict(float)

for account in accounts:
    account_id, account_name = account["id"], account["name"]
    try:
        insights = AdAccount(account_id).get_insights(
            params={
                "time_range": {"since": since_date, "until": until_date},
                "fields": [AdsInsights.Field.spend],
                "breakdowns": ["age", "gender"]
            }
        )

        for insight in insights:
            age_group = insight.get("age", "Unknown")
            gender = insight.get("gender", "Unknown")
            amount_spent = float(insight.get("spend", 0.0))

            if gender == "male":
                gender = "Male"
            elif gender == "female":
                gender = "Female"

            data[(account_id, account_name)][age_group] += amount_spent
            gender_totals[(account_id, account_name, gender)] += amount_spent

    except Exception as e:
        print(f"Error fetching data for account {account_id}: {e}")

# Extract unique age groups
age_groups = sorted({age for values in data.values() for age in values.keys()})

# Create Headers
headers = ["Account ID", "Account Name", "Total Female Spent", "Total Male Spent", "Total Unknown Spent"] + age_groups + ["Updated Time"]

# Write to Google Sheets
sheet.clear()
sheet.append_row(headers)

# Prepare Data for Google Sheet & CSV
rows = []
for (account_id, account_name), age_spent in data.items():
    row = [account_id, account_name]
    row.append(gender_totals.get((account_id, account_name, "Female"), 0.0))  # Total Female
    row.append(gender_totals.get((account_id, account_name, "Male"), 0.0))    # Total Male
    row.append(gender_totals.get((account_id, account_name, "Unknown"), 0.0)) # Total Unknown

    for age_group in age_groups:
        row.append(age_spent.get(age_group, 0.0))  # Fill missing age groups with 0

    row.append(updated_time)  
    rows.append(row)

# Append Data to Google Sheets
if rows:
    sheet.append_rows(rows)

# Write Data to CSV
csv_file = "facebook_ads_data.csv"
with open(csv_file, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(headers)  # Write headers
    writer.writerows(rows)  # Write data

print(f"Data successfully saved to Google Sheets and exported to {csv_file} at {updated_time} UTC.")
