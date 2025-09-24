from datetime import datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError
import csv

# Initialize Facebook API
access_token = "EAAIObOmY9V4BPRHl2TTDsl0CernQP2s1k9ZBWRbx7p8MrgrUC8ZC0z6oy6Q7wXCtKVqr7isy3wrV5EJxttJR1LZCR7AZAKcYrhZCxiardwjc0tvslJx9VhNZBTx6AZB7XtBr6NWGjTQ8RS3VXZB7CY58Mke5YL59QFZBCyrwjKpmtOHXo1r6Yj2W0WVUWeqpxMrZAZA67jTYZBJhAPPb"
FacebookAdsApi.init(access_token=access_token)

# Get yesterday's date
yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
start_date = end_date = yesterday

data = []

try:
    me = User(fbid='me')
    accounts = me.get_ad_accounts(fields=['id', 'name'])
    for account in accounts:
        ad_account_id = account['id']
        ad_account_name = account['name']
        if ad_account_id == 'act_919979366849630' or ad_account_id == "act_1032918741385206" or ad_account_id == "act_842229498117831" or ad_account_id=="act_1952879405139739": 
            continue  # Skip this account
        try:
            ad_account = AdAccount(ad_account_id)
            fields = ['spend', 'actions']
            params = {
                'time_range': {
                    'since': start_date,
                    'until': end_date
                },
                'action_breakdowns': ['action_type']
            }
            insights = ad_account.get_insights(fields=fields, params=params)

            total_spent = 0
            total_leads = 0

            for insight in insights:
                total_spent += float(insight.get('spend', 0))
                actions = insight.get('actions', [])
                for action in actions:
                    if action['action_type'] == 'lead':  
                        total_leads += int(action['value'])

            cpl = total_spent / total_leads if total_leads > 0 else 0

            data.append({
                'Ad Account ID': ad_account_id,
                'Ad Account Name': ad_account_name,
                'Total Spend': total_spent,
                'Total Leads': total_leads,
                'Cost Per Lead (CPL)': cpl
            })

        except FacebookRequestError as e:
            print(f"Error fetching insights for Ad Account {ad_account_id} ({ad_account_name}): {e.api_error_message()}")
            data.append({
                'Ad Account ID': ad_account_id,
                'Ad Account Name': ad_account_name,
                'Total Spend': 'Error',
                'Total Leads': 'Error',
                'Cost Per Lead (CPL)': 'Error'
            })

except FacebookRequestError as e:
    print("An error occurred while fetching ad accounts:")
    print(f"Message: {e.api_error_message()}")

# Define file path dynamically based on yesterday's date
csv_file = rf"D:\2024\spent\sep\spent{yesterday}.csv"
with open(csv_file, mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=['Ad Account ID', 'Ad Account Name', 'Total Spend', 'Total Leads', 'Cost Per Lead (CPL)'])
    writer.writeheader()
    writer.writerows(data)

print(f"Report saved to {csv_file}")
