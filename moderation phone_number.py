import requests
import json
import re
import gspread
from datetime import datetime, timezone, timedelta
from oauth2client.service_account import ServiceAccountCredentials

# Disable SSL verification
session = requests.Session()
session.verify = False
requests.packages.urllib3.disable_warnings()

# Example request with disabled SSL verification
response = session.get("https://www.googleapis.com/drive/v3/files")
# Google Sheets Setup
SHEET_NAME = "Number Scraping"
YESTERDAY = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")  # Yesterday's date
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDENTIALS_FILE = r"D:\2024\striped-sunspot-451315-t6-343357709c71.json"  # Update with your actual credentials file

# Authenticate Google Sheets
creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPES)
client = gspread.authorize(creds)

# Open spreadsheet and create a worksheet if not exists
spreadsheet = client.open(SHEET_NAME)
try:
    sheet = spreadsheet.worksheet(YESTERDAY)
except gspread.exceptions.WorksheetNotFound:
    sheet = spreadsheet.add_worksheet(title=YESTERDAY, rows="1000", cols="20")

# Facebook API Credentials
pages = {
    "OK ar/en":"EAAIObOmY9V4BO6b0DeXknz5R0eiuiYZCmAr9v0IUYFtG1WW4tZBPGqV9xjLYFq7LjLRyWh1jf93Cjd9IhLiZCmbKDJvZATVLJ0ZC0Ril6XjHwrZB8in2IsXbSh2byRYe2z0TTsoGAmryQUMdbuVLvsXZBPt2SDQ9eK8ubbP6IvlPf9e2Uvkjvd0ClEQGm6TrpJVhpiM1AVUO2O6sgb1LG2XCSQXnZAI4GyvGQ68eq2Iq",
    "Kbeera": "EAAIObOmY9V4BO07XmDBfOYyZAOaQ1ZBkWms67TETEpRObmsPRIx0ZBn8guC9lzs5ZAxiZB9WZCv6zZAv5OGGq3ez31QZCEmrgT28lpJrOVANK48MnvtXPjHg8SAZAQEZC9hGKI5CTgRZBn05S04AqMpmZBKTNmlZAxfQ2oDhF65UsTNBGZACFZAFi3KUqJT4HOL41Yfmg1UlZCqgXDD9UoCrnyyMxMga6a9i54KZApdBR6McZD",
    "Tarkebat": "EAAIObOmY9V4BO7YQMXtzptb9YFDg0PZAmAitoxZBmnCahaEtKpWUKm4vNP91JeCFRpCZCfyKgXplqnBZB9YjGoAQZANmKfzIoUX0W7mMrCQkv1uTdSSzjOFOyxRaeeigcxJo0a2ob7JM8Hb1ztNSxlsmH4dCJPmoKtyu9Na1YA9FuewZCcYmxEZAlUV2xWi0aaQASImytHLHY0vOC9uZC2gXj4PTuExQqEjrYMfY"
}

# Time Range for Yesterday (12 PM to 11:59 PM UTC)
START_TIME = datetime.strptime(f"{YESTERDAY} 12:00:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
END_TIME = datetime.strptime(f"{YESTERDAY} 23:59:59", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

def fetch_messages(url):
    all_messages = []
    while url:
        try:
            response = requests.get(url, timeout=30)  # Set a longer timeout
            response.raise_for_status()  # Raise an error for HTTP issues
            data = response.json()

            if "data" in data:
                for convo in data["data"]:
                    if "messages" in convo and "data" in convo["messages"]:
                        for msg in convo["messages"]["data"]:
                            message_text = msg.get("message", "")
                            created_time = msg.get("created_time", "")

                            if created_time:
                                msg_datetime = datetime.fromisoformat(created_time.replace("Z", "+00:00"))
                                if START_TIME <= msg_datetime <= END_TIME:
                                    all_messages.append(message_text)

            url = data.get("paging", {}).get("next")  # Move to next page if available
            # print(f"Next page: {url}")  # Debugging

        except requests.exceptions.RequestException as e:
            print(f"Error fetching messages: {e}")
            break  # Stop retrying if there's an error

    return all_messages

# Phone Number Extraction
phone_pattern = re.compile(r'\b(?:\+?[0-9\u0660-\u0669]{1,3}[-.\s]?)?(?:\(?[0-9\u0660-\u0669]{2,4}\)?[-.\s]?)?[0-9\u0660-\u0669]{3,4}[-.\s]?[0-9\u0660-\u0669]{4}\b')
arabic_digits_map = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

page_results = {}

for page_name, token in pages.items():
    URL = f"https://graph.facebook.com/v18.0/me/conversations?fields=messages{{message,created_time}}&access_token={token}"
    print(f"Fetching messages for {page_name}...")
    
    all_messages = fetch_messages(URL)
    print(f"Total messages fetched for {page_name}: {len(all_messages)}")
    
    phone_numbers = set()
    for message in all_messages:
        print(f"Checking message: {message}")  # Debugging line
        normalized_message = message.translate(arabic_digits_map)
        extracted_numbers = phone_pattern.findall(normalized_message)
        phone_numbers.update(num for num in extracted_numbers if len(num) != 15)
    
    print(f"Extracted {len(phone_numbers)} phone numbers for {page_name}")
    page_results[page_name] = list(phone_numbers)

# Update Google Sheet
data_to_insert = []
data_to_insert.append([page_name for page_name in page_results.keys()])
max_length = max(len(numbers) for numbers in page_results.values())

for i in range(max_length):
    row = []
    for page_name in page_results.keys():
        row.append(page_results[page_name][i] if i < len(page_results[page_name]) else "")
    data_to_insert.append(row)

sheet.update("A1", data_to_insert)
print(f"Updated Google Sheet '{SHEET_NAME}' with phone numbers for {YESTERDAY}")
