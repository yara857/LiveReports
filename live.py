import pandas as pd
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError
from datetime import datetime
import csv
access_token = "EAAIObOmY9V4BO72lw6KMeXaJbmD0UqlbVAiAcys2uJWxpr6AM4O5Y6qriuYufXwX4UNSmCyWozfKZCt9Ebe8e89hIqKvs1ZCWBbQlxDZBe8TMDEHX7oWkgoaZAZAKt0ZBO9xZCRgHRVgir6lHRVmqo2hZAJ2LCavXVlOHZCndGvsYluSsZCdFxi0OFmZBtJd31nITjxbIroH5VB"
FacebookAdsApi.init(access_token=access_token)

# Get today's date dynamically
today_date = datetime.today().strftime('%Y-%m-%d')

# Specify the date range
start_date = today_date  
end_date = today_date 

# List to store data
data = []

try:
    # Fetch Ad Accounts for the user
    me = User(fbid="me")
    accounts = me.get_ad_accounts(fields=["id", "name"])

    for account in accounts:
        ad_account_id = account["id"]
        ad_account_name = account["name"]
        print(f"Processing Ad Account: {ad_account_name} ({ad_account_id})")

        try:
            ad_account = AdAccount(ad_account_id)

            # Insights query parameters
            fields = ["spend", "actions"]
            params = {
                "time_range": {
                    "since": start_date,
                    "until": end_date,
                },
                "action_breakdowns": ["action_type"],
                "limit": 1000,  # Retrieve more data if available
            }

            # Fetch insights with pagination
            insights = []
            insights_data = ad_account.get_insights(fields=fields, params=params)
            while insights_data:
                insights.extend(insights_data)

            # Process insights
            total_spent = 0
            total_leads = 0

            for insight in insights:
                total_spent += float(insight.get("spend", 0))

                # Print all actions for debugging
                actions = insight.get("actions", [])
                print(f"Actions: {actions}")  # Debugging step

                for action in actions:
                    if action["action_type"] in ["lead"]:
                        total_leads += int(action["value"])

            # Calculate CPL (Cost Per Lead)
            cpl = total_spent / total_leads if total_leads > 0 else 0

            # Store in data list
            data.append({
                "Ad Account ID": ad_account_id,
                "Ad Account Name": ad_account_name,
                "Total Spend": total_spent,
                "Total Leads": total_leads,
                "Cost Per Lead (CPL)": cpl,
            })

        except FacebookRequestError as e:
            print(f"Error fetching insights for {ad_account_name} ({ad_account_id}): {e.api_error_message()}")
            data.append({
                "Ad Account ID": ad_account_id,
                "Ad Account Name": ad_account_name,
                "Total Spend": "Error",
                "Total Leads": "Error",
                "Cost Per Lead (CPL)": "Error",
            })

except FacebookRequestError as e:
    print("An error occurred while fetching ad accounts:")
    print(f"Message: {e.api_error_message()}")
    print(f"Error Code: {e.api_error_code}")
    print(f"Error Subcode: {e.api_error_subcode()}")

# Save data to CSV
csv_file = fr"D:\2024\spent\spent{today_date}.csv"
with open(csv_file, mode="w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=["Ad Account ID", "Ad Account Name", "Total Spend", "Total Leads", "Cost Per Lead (CPL)"])
    writer.writeheader()
    writer.writerows(data)

print(f"Report saved to {csv_file}")
