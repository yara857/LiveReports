import os
import json
import gspread
import pandas as pd
import requests
from oauth2client.service_account import ServiceAccountCredentials
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from datetime import datetime, timedelta
from config import FACEBOOK_ACCESS_TOKEN

FacebookAdsApi.init(access_token=FACEBOOK_ACCESS_TOKEN)
me = User(fbid="me")
accounts = me.get_ad_accounts(fields=["id", "name"])


# Authenticate with Google Sheets
scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("striped-sunspot-451315-t6-8b0e56f96486.json", scope)
client = gspread.authorize(creds)

# Open Google Sheet
spreadsheet = client.open("Live Report")
sheet = spreadsheet.sheet1  

headers = [
    "Account ID", "Account Name", "Daily Budget", "Yesterday Spent", "Yesterday Leads",
    "Yesterday CPL", "Today Spent", "Today Leads", "Today CPL",
    "Total Spent (Month)", "Total Leads (Month)", "CPL (Month)", "Last Updated"
]
sheet.clear()
sheet.append_row(headers)

# Get Dates
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
today = datetime.now().strftime("%Y-%m-%d")
first_of_month = datetime.now().replace(day=1).strftime("%Y-%m-%d")
last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def fetch_daily_budget(account_id):
    url = f"https://graph.facebook.com/v18.0/{account_id}/adsets?fields=daily_budget&access_token={access_token}"
    response = requests.get(url)
    if response.status_code == 200:
        adsets = response.json().get("data", [])
        return sum(int(adset["daily_budget"]) for adset in adsets if adset.get("daily_budget"))
    print(f"Error fetching budget for {account_id}: {response.status_code}, {response.text}")
    return 0

data = []
for account in accounts:
    account_id, account_name = account["id"], account["name"]
    
    def fetch_insights(account_id, since, until):
        return AdAccount(account_id).get_insights(params={
            "time_range": {"since": since, "until": until},
            "fields": [AdsInsights.Field.spend, AdsInsights.Field.actions],
        })

    def process_insights(insights):
        spent, leads = "0.00", 0
        for insight in insights:
            spent = insight.get("spend", "0.00")
            for action in insight.get("actions", []):
                if action["action_type"] == "lead":
                    leads = int(action.get("value", 0))
        return spent, leads, round(float(spent) / leads, 2) if leads else "N/A"

    try:
        yesterday_spent, yesterday_leads, yesterday_cpl = process_insights(fetch_insights(account_id, yesterday, yesterday))
        today_spent, today_leads, today_cpl = process_insights(fetch_insights(account_id, today, today))
        total_spent_month, total_leads_month, cpl_month = process_insights(fetch_insights(account_id, first_of_month, today))

        daily_budget = fetch_daily_budget(account_id) / 100
        data.append([
            account_id, account_name, daily_budget, yesterday_spent, yesterday_leads, yesterday_cpl,
            today_spent, today_leads, today_cpl, total_spent_month, total_leads_month, cpl_month, last_updated
        ])

    except Exception as e:
        print(f"Error fetching data for {account_id}: {e}")
        data.append([account_id, account_name, "Error"] * 11 + [last_updated])

# Append to Google Sheets
if data:
    sheet.append_rows(data)

print("Data successfully saved to Google Sheets.")
