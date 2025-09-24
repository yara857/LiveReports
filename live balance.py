import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount

# === Facebook API Setup ===
access_token = "EAAIObOmY9V4BPRHl2TTDsl0CernQP2s1k9ZBWRbx7p8MrgrUC8ZC0z6oy6Q7wXCtKVqr7isy3wrV5EJxttJR1LZCR7AZAKcYrhZCxiardwjc0tvslJx9VhNZBTx6AZB7XtBr6NWGjTQ8RS3VXZB7CY58Mke5YL59QFZBCyrwjKpmtOHXo1r6Yj2W0WVUWeqpxMrZAZA67jTYZBJhAPPb"
FacebookAdsApi.init(access_token=access_token)

# === Google Sheets Setup ===
scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(
    r"C:\Users\essam\Downloads\striped-sunspot-451315-t6-343357709c71.json", scope
)
client = gspread.authorize(creds)

# Open the Google Sheet named "live balance"
spreadsheet_name = "live balance"
sheet = client.open(spreadsheet_name).sheet1  # First sheet

# === Function to Get Prepaid Balance ===
def get_balance_details(ad_account_id):
    try:
        account = AdAccount(ad_account_id)
        data = account.api_get(fields=["funding_source_details"])
        display_string = data.get("funding_source_details", {}).get("display_string", "Not Found")

        if "Visa" in display_string or "Mastercard" in display_string:
            return "Excluded"

        match = re.search(r"[-+]?[0-9,]*\.\d+|[-+]?[0-9,]+", display_string)
        return float(match.group().replace(",", "")) if match else None
    except Exception as e:
        error_message = str(e)
        if "Permission Denied" in error_message:
            return "Permission Denied"
        return f"Error: {error_message}"


# === Get Ad Accounts and Write to Sheet ===
me = User(fbid='me')
accounts = me.get_ad_accounts(fields=['id', 'name'])

# Prepare data
rows = [["Account Name", "Account ID", "Balance"]]
try:
    me = User(fbid='me')
    accounts = me.get_ad_accounts(fields=['id', 'name'])
    
    for account in accounts:
        try:
            acc_id = account["id"]
            acc_name = account["name"]
            balance = get_balance_details(acc_id)
            rows.append([acc_name, acc_id, balance])
        except Exception as acc_err:
            rows.append([f"Error reading account", "N/A", str(acc_err)])
            continue

except Exception as outer_err:
    rows.append(["Failed to fetch accounts", "N/A", str(outer_err)])

# Clear and update sheet
try:
    sheet.clear()
    sheet.update("A1", rows)
    print("Google Sheet updated successfully.")
except Exception as sheet_err:
    print(f"Google Sheet update failed: {sheet_err}")