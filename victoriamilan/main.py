import asyncio
import csv
import json
from datetime import datetime
from pathlib import Path

import gspread
import httpx
from aiolimiter import AsyncLimiter
from oauth2client.service_account import ServiceAccountCredentials
from selectolax.parser import HTMLParser

EMAIL = "INSERT_EMAIL_HERE"  # for google sheets
COOKIE = "INSERT_COOKIE_HERE"
USER_AGENT = "INSERT_USER_AGENT_HERE"


def format_date(date_str):
    # Parse the date string into a datetime object
    date_obj = datetime.strptime(date_str, "%b %d, %Y")

    # Format the datetime object into the desired format
    return date_obj.strftime("%d/%m/%Y")


async def fetch(client, url, limiter, queue):
    headers = {
        "cookie": COOKIE,
        "user-agent": USER_AGENT,
    }

    async with limiter:
        try:
            res = await client.get(url, headers=headers, timeout=30.0)  # Set timeout to 30 seconds
            if res.status_code != 200:
                print("Error: Page not loaded properly")
                return None
            await queue.put((res.text, url))
        except httpx.ReadTimeout:
            print(f"ReadTimeout: Failed to fetch {url}")
        except Exception as e:
            print(f"Exception: {e}")


def process_html(html, link):
    data = []
    tree = HTMLParser(html)
    id = tree.css_first(".user-id").text().strip().lstrip("#").split()[0]
    user_name = tree.css_first(".profile-info").css_first("a").text()
    account_type = tree.css_first(".account-status").text().strip()
    media_td_divs = tree.css_first("td.media").css("div")
    registered_found = False
    for div in media_td_divs:
        if "Registered:" == div.text() and registered_found is False:
            registered_found = True
            continue
        if registered_found:
            registered_date = div.text()
            break
    data.append(
        {
            "id": id,
            "user_name": user_name,
            "account_type": account_type,
            "registered_date": format_date(registered_date),
            "link": link,
            "created_at": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        }
    )
    return data


async def process_queue(queue):
    output_file = Path("output.json")
    output_data = []

    while True:
        try:
            html, link = await queue.get()
        except TypeError:
            break
        if html is None:
            break

        data = process_html(html, link)
        output_data.extend(data)

        # Append to the JSON file
        with output_file.open("w") as f:
            json.dump(output_data, f, indent=4)

        queue.task_done()


def get_google_sheet_client():

    # Define the scope
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

    # Load the credentials
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)

    # Authorize the clientsheet
    return gspread.authorize(creds)


def save_to_google_sheet():
    # Get the current date and time
    now = datetime.now()

    # Format the date and time as yyyy-mm-dd_hourmin
    timestamp = now.strftime("%Y-%m-%d_%H%M")

    # Open the Google Sheet
    filename = f"victoriamilan_{timestamp}"
    gsc = get_google_sheet_client()
    sheet = gsc.create(filename).sheet1

    # Share the Google Sheet with your email
    gsc.insert_permission(sheet.spreadsheet.id, EMAIL, perm_type="user", role="writer")

    # Read data from the JSON file
    with open("output.json", "r") as f:
        data = json.load(f)

    # Extract headers dynamically from the JSON data
    if data:
        headers = list(data[0].keys())
        # Add headers to the sheet
        sheet.append_row(headers)

        # Append each row of data to the sheet
        for entry in data:
            row = [entry.get(header, None) for header in headers]
            sheet.append_row(row)


async def main():
    rate_limit = AsyncLimiter(5, 1)  # number of request per number of second
    queue = asyncio.Queue()
    async with httpx.AsyncClient() as client:
        tasks = []
        with open("links.csv") as file:
            csv_reader = csv.reader(file)
            for row in csv_reader:
                tasks.append(fetch(client, row[0], rate_limit, queue))

        asyncio.create_task(process_queue(queue))

        await asyncio.gather(*tasks)

        # Signal the consumer to stop
        await queue.put(None)

    save_to_google_sheet()


if __name__ == "__main__":
    asyncio.run(main())
