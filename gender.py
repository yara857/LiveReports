import csv
import gspread
import pandas as pd
from datetime import datetime ,timezone ,timedelta
from collections import defaultdict
from oauth2client.service_account import ServiceAccountCredentials
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights

# Facebook API Setup
access_token = "EAAIObOmY9V4BPRHl2TTDsl0CernQP2s1k9ZBWRbx7p8MrgrUC8ZC0z6oy6Q7wXCtKVqr7isy3wrV5EJxttJR1LZCR7AZAKcYrhZCxiardwjc0tvslJx9VhNZBTx6AZB7XtBr6NWGjTQ8RS3VXZB7CY58Mke5YL59QFZBCyrwjKpmtOHXo1r6Yj2W0WVUWeqpxMrZAZA67jTYZBJhAPPb"
FacebookAdsApi.init(access_token=access_token)
me = User(fbid='me')
accounts = me.get_ad_accounts(fields=['id', 'name'])

# ========== Google Sheets Setup ==========
scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(
    r"C:\Users\essam\Downloads\striped-sunspot-451315-t6-343357709c71.json", scope)
client = gspread.authorize(creds)
spreadsheet = client.open("gender spent")

# ========== Define Teams ==========
teams = {
    "vibes team": {
        "act_446143675092721", "act_3735033723398649", "act_1160917978500634",
        "act_1438264286880701", "act_913634477428214", "act_1257976282245126",
        "act_1500284387257195", "act_880045560717824", "act_2223009838063616",
        "act_503501265565688", "act_472782225604827"
    },
    "qaoud team": {
        "act_1110493866779936", "act_2552907308215563", "act_803931941918015",
        "act_491661666975935", "act_3783876785259320", "act_808836814588501",
        "act_349419674831825", "act_605590368701495", "act_1106920700810737",
        "act_3917900671783321" ,'act_1032918741385206' , 'act_1873860226413296','act_820067853430864',
        'act_525111343230393',"act_1032918741385206"
    },
    "taher team": {
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

# ========== Date Setup ==========
today = datetime.today()
first_of_month = today.replace(day=1)
since_date = first_of_month.strftime('%Y-%m-%d')
until_date = today.strftime('%Y-%m-%d')
egypt_offset = timedelta(hours=3)
updated_time = (datetime.utcnow() + egypt_offset).strftime('%Y-%m-%d %H:%M:%S')


# ========== Data Structures ==========
team_gender_totals = defaultdict(lambda: defaultdict(float))
team_age_spent = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))

# ========== Fetch Insights ==========
for account in accounts:
    account_id = account['id']
    account_name = account['name']

    # Identify team
    team_name = next((team for team, ids in teams.items() if account_id in ids), None)
    if not team_name:
        continue  # Skip accounts not in any team

    try:
        insights = AdAccount(account_id).get_insights(
            params={
                'time_range': {'since': since_date, 'until': until_date},
                'fields': [AdsInsights.Field.spend],
                'breakdowns': ['age', 'gender']
            }
        )

        for insight in insights:
            age = insight.get('age', 'Unknown')
            gender = insight.get('gender', 'Unknown').capitalize()
            spend = float(insight.get('spend', 0.0))

            team_age_spent[team_name][(account_id, account_name)][age] += spend
            team_gender_totals[team_name][(account_id, account_name, gender)] += spend

    except Exception as e:
        print(f"Error fetching data for account {account_id}: {e}")

# ========== Determine All Age Groups ==========
age_groups = sorted({age for team in team_age_spent.values() for acct in team.values() for age in acct})

# ========== Write Data to Sheets ==========
for team_name in teams:
    # Get or create sheet
    try:
        sheet = spreadsheet.worksheet(team_name)
    except:
        sheet = spreadsheet.add_worksheet(title=team_name, rows="1000", cols="50")

    # Clear sheet
    sheet.clear()

    # Create headers
    headers = ['Account ID', 'Account Name', 'Total Female Spent', 'Total Male Spent', 'Total Unknown Spent'] + age_groups + ['Updated Time']
    sheet.append_row(headers)

    # Build rows
    rows = []
    for (account_id, account_name), age_spending in team_age_spent[team_name].items():
        row = [account_id, account_name]
        row.append(team_gender_totals[team_name].get((account_id, account_name, 'Female'), 0.0))
        row.append(team_gender_totals[team_name].get((account_id, account_name, 'Male'), 0.0))
        row.append(team_gender_totals[team_name].get((account_id, account_name, 'Unknown'), 0.0))
        for age in age_groups:
            row.append(age_spending.get(age, 0.0))
        row.append(updated_time)
        rows.append(row)

    if rows:
        sheet.append_rows(rows)

print(f"✅ Data from {since_date} to {until_date} successfully written to Google Sheets at {updated_time} UTC.")