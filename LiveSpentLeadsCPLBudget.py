import gspread
import pandas as pd
import requests
import os
from oauth2client.service_account import ServiceAccountCredentials
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from datetime import datetime, timedelta

# Facebook API Setup
access_token = os.environ.get("FB_ACCESS_TOKEN")
FacebookAdsApi.init(access_token=access_token)

me = User(fbid='me')
accounts = me.get_ad_accounts(fields=['id', 'name'])

# Google Sheets Setup
scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("striped-sunspot-451315-t6-8b0e56f96486.json", scope)
client = gspread.authorize(creds)

# Open Google Sheet (Replace with your Sheet Name)
spreadsheet = client.open("Live Report")
sheet = spreadsheet.sheet1  # Access first sheet

headers = ['Account ID', 'Account Name','Daily Budget', 'Yesterday Spent', 'Yesterday Leads', 'Yesterday CPL', 'Today Spent', 'Today Leads', 'Today CPL',  'Total Spent (Month)', 'Total Leads (Month)', 'CPL (Month)', 'Last Updated']
sheet.clear()
sheet.append_row(headers)

# Get Dates
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
today = datetime.now().strftime('%Y-%m-%d')
first_of_month = datetime.now().replace(day=1).strftime('%Y-%m-%d')
last_updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def fetch_daily_budget(account_id):
    url = f"https://graph.facebook.com/v18.0/{account_id}/adsets?fields=daily_budget&access_token={access_token}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        adsets = data.get('data', [])
        total_budget = sum(int(adset['daily_budget']) for adset in adsets if adset.get('daily_budget'))
        return total_budget
    else:
        print(f"Error fetching budget for account {account_id}: {response.status_code}, {response.text}")
        return 0

data = []
for account in accounts:
    account_id = account['id']
    account_name = account['name']
    
    try:
        # Fetch Yesterday's Data
        insights_yesterday = AdAccount(account_id).get_insights(
            params={
                'time_range': {'since': yesterday, 'until': yesterday},
                'fields': [AdsInsights.Field.spend, AdsInsights.Field.actions],
            }
        )
        
        yesterday_spent = '0.00'
        yesterday_leads = 0
        yesterday_cpl = 'N/A'
        
        for insight in insights_yesterday:
            yesterday_spent = insight.get('spend', '0.00')
            actions = insight.get('actions', [])
            for action in actions:
                if action['action_type'] == 'lead':
                    yesterday_leads = int(action.get('value', 0))
            if yesterday_leads > 0:
                yesterday_cpl = round(float(yesterday_spent) / yesterday_leads, 2)
        
        # Fetch Today's Data
        insights_today = AdAccount(account_id).get_insights(
            params={
                'time_range': {'since': today, 'until': today},
                'fields': [AdsInsights.Field.spend, AdsInsights.Field.actions],
            }
        )
        
        today_spent = '0.00'
        today_leads = 0
        today_cpl = 'N/A'
        
        for insight in insights_today:
            today_spent = insight.get('spend', '0.00')
            actions = insight.get('actions', [])
            for action in actions:
                if action['action_type'] == 'lead':
                    today_leads = int(action.get('value', 0))
            if today_leads > 0:
                today_cpl = round(float(today_spent) / today_leads, 2)
        
        # Fetch Total Spent and Leads for the Month
        insights_month = AdAccount(account_id).get_insights(
            params={
                'time_range': {'since': first_of_month, 'until': today},
                'fields': [AdsInsights.Field.spend, AdsInsights.Field.actions],
            }
        )
        
        total_spent_month = '0.00'
        total_leads_month = 0
        cpl_month = 'N/A'
        
        for insight in insights_month:
            total_spent_month = insight.get('spend', '0.00')
            actions = insight.get('actions', [])
            for action in actions:
                if action['action_type'] == 'lead':
                    total_leads_month = int(action.get('value', 0))
            if total_leads_month > 0:
                cpl_month = round(float(total_spent_month) / total_leads_month, 2)
        
        # Fetch Daily Budget
        daily_budget = fetch_daily_budget(account_id)
        
        data.append([account_id, account_name, daily_budget/100, yesterday_spent, yesterday_leads, yesterday_cpl, today_spent, today_leads, today_cpl,  total_spent_month, total_leads_month, cpl_month, last_updated])
    
    except Exception as e:
        print(f"Error fetching data for account {account_id}: {e}")
        data.append([account_id, account_name, 'Error', 'Error', 'Error', 'Error', 'Error', 'Error', 'Error', 'Error', 'Error', 'Error', last_updated])

# Append data to Google Sheets
if data:
    sheet.append_rows(data)

print("Data successfully saved to Google Sheets.")
