import csv
import gspread
import pandas as pd
from datetime import datetime
import os
import json
from collections import defaultdict
from oauth2client.service_account import ServiceAccountCredentials
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights

# Facebook API Setup
access_token = os.environ.get("FB_ACCESS_TOKEN")FacebookAdsApi.init(access_token=access_token)

me = User(fbid='me')
accounts = me.get_ad_accounts(fields=['id', 'name'])

google_credentials = os.environ["GOOGLE_SHEET_CREDENTIALS"]
creds_dict = json.loads(google_credentials)
# Google Sheets Setup
scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)
# Open Google Sheet (Replace with your Sheet Name)
spreadsheet = client.open("live spent with gender and age")
sheet = spreadsheet.sheet1  # Access first sheet

# Get Date Range (First of the month to today)
today = datetime.today()
first_of_month = today.replace(day=1)
since_date = first_of_month.strftime('%Y-%m-%d')
until_date = today.strftime('%Y-%m-%d')
updated_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')  # Current UTC time

# Fetch Data
data = defaultdict(lambda: defaultdict(float))  # Dictionary to store summed values
gender_totals = defaultdict(float)  # Dictionary to store total gender-wise spending

for account in accounts:
    account_id = account['id']
    account_name = account['name']
    try:
        insights = AdAccount(account_id).get_insights(
            params={
                'time_range': {'since': since_date, 'until': until_date},
                'fields': [AdsInsights.Field.spend],
                'breakdowns': ['age', 'gender']
            }
        )

        for insight in insights:
            age_group = insight.get('age', 'Unknown')
            gender = insight.get('gender', 'Unknown')
            amount_spent = float(insight.get('spend', 0.0))

            # Normalize gender values
            if gender == 'male':
                gender = 'Male'
            elif gender == 'female':
                gender = 'Female'

            # Sum up by age group
            data[(account_id, account_name)][age_group] += amount_spent
            gender_totals[(account_id, account_name, gender)] += amount_spent

    except Exception as e:
        print(f"Error fetching data for account {account_id}: {e}")

# Extract unique age groups
age_groups = sorted({age for values in data.values() for age in values.keys()})

# Create Headers
headers = ['Account ID', 'Account Name', 'Total Female Spent', 'Total Male Spent', 'Total Unknown Spent'] + age_groups + ['Updated Time']
sheet.clear()  # Clear existing data before writing
sheet.append_row(headers)

# Prepare Data for Google Sheet
rows = []
for (account_id, account_name), age_spent in data.items():
    row = [account_id, account_name]
    
    # Add gender-wise totals **immediately after account name**
    row.append(gender_totals.get((account_id, account_name, 'Female'), 0.0))  # Total Female
    row.append(gender_totals.get((account_id, account_name, 'Male'), 0.0))    # Total Male
    row.append(gender_totals.get((account_id, account_name, 'Unknown'), 0.0)) # Total Unknown

    # Add age group breakdowns
    for age_group in age_groups:
        row.append(age_spent.get(age_group, 0.0))  # Fill missing age groups with 0
    
    row.append(updated_time)  # Add update time
    rows.append(row)

# Append Data to Google Sheets
if rows:
    sheet.append_rows(rows)

print(f"Data from {since_date} to {until_date} successfully saved to Google Sheets at {updated_time} UTC.")
