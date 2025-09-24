import gspread
import csv
import re
import pandas as pd
import requests
from collections import defaultdict
from oauth2client.service_account import ServiceAccountCredentials
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from datetime import datetime, timedelta

# Facebook API Setup
ACCESS_TOKEN = "EAAIObOmY9V4BPRHl2TTDsl0CernQP2s1k9ZBWRbx7p8MrgrUC8ZC0z6oy6Q7wXCtKVqr7isy3wrV5EJxttJR1LZCR7AZAKcYrhZCxiardwjc0tvslJx9VhNZBTx6AZB7XtBr6NWGjTQ8RS3VXZB7CY58Mke5YL59QFZBCyrwjKpmtOHXo1r6Yj2W0WVUWeqpxMrZAZA67jTYZBJhAPPb"
FacebookAdsApi.init(access_token=ACCESS_TOKEN)

me = User(fbid='me')
accounts = me.get_ad_accounts(fields=['id', 'name'])

# Team mappings
teams = {
    "vibes": {
        "act_446143675092721", "act_3735033723398649", "act_1160917978500634",
        "act_1438264286880701", "act_913634477428214", "act_1257976282245126",
        "act_1500284387257195", "act_880045560717824", "act_2223009838063616",
        "act_503501265565688", "act_472782225604827"
    },
    "qaoud": {
        "act_1110493866779936", "act_2552907308215563", "act_803931941918015",
        "act_491661666975935", "act_3783876785259320", "act_808836814588501",
        "act_349419674831825", "act_605590368701495", "act_1106920700810737",
        "act_3917900671783321" ,'act_1032918741385206' , 'act_1873860226413296','act_820067853430864',
        'act_1832609284326278', "act_1831681447773953"
        'act_525111343230393',"act_1032918741385206"
    },
    "taher": {
        "act_1122033631798415", "act_1573944913033077", "act_1449776175430067",
        "act_461392002996920", "act_378525045233709", "act_941916861325221",
        "act_1925625627955953", "act_850056290595951", "act_3876212252698351",
        "act_7949479881827692", "act_577926371246983", "act_1807970279737291",
        "act_1069345594937651", "act_2227598254277390", "act_537517202274301",
        "act_1193325865062689", "act_1652622545469088", "act_1033571434639050",
        "act_860397696266577", "act_569341712749669", "act_1747046902753277",
        "act_1097303645310610", "act_517798987807178", "act_1624624092271535",
        "act_501956059557382", "act_399947229848173", "act_2041103139723714",
        "act_1391794181816851", "act_613618428335002", "act_1177029914077193",
        "act_1365280811345488", "act_663917829898406" , "act_9204702042967493" , 
        "act_663917829898406" , "act_501956059557382" , "act_1365280811345488" ,
        "act_1279091416916908"
    }
}

# Google Sheets Setup
scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(r"C:\Users\essam\Downloads\striped-sunspot-451315-t6-343357709c71.json", scope)
client = gspread.authorize(creds)

spreadsheet = client.open("Live")

# Time setup
today = datetime.now().strftime('%Y-%m-%d')
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
first_of_month = datetime.now().replace(day=1).strftime('%Y-%m-%d')
last_updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Balance function
def get_balance_details(ad_account_id):
    try:
        account = AdAccount(ad_account_id)
        data = account.api_get(fields=["funding_source_details"])
        display_string = data.get("funding_source_details", {}).get("display_string", "Not Found")
        if "Visa" in display_string or "Mastercard" in display_string:
            return "Excluded"
        match = re.search(r"[-+]?[0-9,]*\.\d+|[-+]?[0-9,]+", display_string)
        return float(match.group().replace(",", "")) if match else None
    except Exception:
        return "Error"

# Process and group by team
team_data = defaultdict(list)

for account in accounts:
    account_id = account["id"]
    account_name = account["name"]
    if account_id in {"act_919979366849630", "act_1032918741385206", "act_842229498117831", "act_1952879405139739"}:
        continue  # Skip accounts

    try:
        # Yesterday
        insights_yesterday = AdAccount(account_id).get_insights(params={
            "time_range": {"since": yesterday, "until": yesterday},
            "fields": [AdsInsights.Field.spend, AdsInsights.Field.actions]
        })
        y_spent, y_leads = "0.00", 0
        for i in insights_yesterday:
            y_spent = i.get("spend", "0.00")
            for a in i.get("actions", []):
                if a["action_type"] == "lead":
                    y_leads = int(a.get("value", 0))
        y_cpl = round(float(y_spent) / y_leads, 2) if y_leads > 0 else "N/A"

        # Today
        insights_today = AdAccount(account_id).get_insights(params={
            "time_range": {"since": today, "until": today},
            "fields": [AdsInsights.Field.spend, AdsInsights.Field.actions]
        })
        t_spent, t_leads = "0.00", 0
        for i in insights_today:
            t_spent = i.get("spend", "0.00")
            for a in i.get("actions", []):
                if a["action_type"] == "lead":
                    t_leads = int(a.get("value", 0))
        t_cpl = round(float(t_spent) / t_leads, 2) if t_leads > 0 else "N/A"

        # Month overall
        insights_overall = AdAccount(account_id).get_insights(params={
            "time_range": {"since": first_of_month, "until": today},
            "fields": [AdsInsights.Field.spend, AdsInsights.Field.actions]
        })
        o_spent, o_leads = "0.00", 0
        for i in insights_overall:
            o_spent = i.get("spend", "0.00")
            for a in i.get("actions", []):
                if a["action_type"] == "lead":
                    o_leads = int(a.get("value", 0))
        o_cpl = round(float(o_spent) / o_leads, 2) if o_leads > 0 else "N/A"

        # Balance
        balance = get_balance_details(account_id)
        def parse_number(val):
            try:
                if isinstance(val, str) and val.upper() == "N/A":
                    return None
                if '.' in str(val):
                    return float(val)
                return int(val)
            except:
                return val
            
        row = [
            str(account_id),
            str(account_name),
            parse_number(y_spent),
            parse_number(y_leads),
            parse_number(y_cpl),
            parse_number(t_spent),
            parse_number(t_leads),
            parse_number(t_cpl),
            parse_number(o_spent),
            parse_number(o_leads),
            parse_number(o_cpl),
            parse_number(balance),
            str(last_updated)
        ]

        # Determine team
        for team, ids in teams.items():
            if account_id in ids:
                team_data[team].append(row)
                break

    except Exception as e:
        print(f"Error with {account_id}: {e}")

# Headers (no budget column)
headers = ['Account ID', 'Account Name',
           'Yesterday Spent', 'Yesterday Leads', 'Yesterday CPL',
           'Today Spent', 'Today Leads', 'Today CPL',
           'Overall Spent', 'Overall Leads', 'Overall CPL',
           'Available Balance', 'Last Updated']

# Write data to sheets
for team, rows in team_data.items():
    try:
        try:
            sheet = spreadsheet.worksheet(team)
        except:
            sheet = spreadsheet.add_worksheet(title=team, rows="1000", cols="20")
        sheet.clear()
        sheet.append_row(headers)
        sheet.append_rows(rows)
    except Exception as e:
        print(f"Failed to write to sheet '{team}': {e}")

print("Team-wise data (without budget) successfully saved to Google Sheets.")