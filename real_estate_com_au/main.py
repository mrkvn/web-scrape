import asyncio
import json
from pathlib import Path
from urllib.parse import urlparse

import httpx
from aiolimiter import AsyncLimiter
from selectolax.parser import HTMLParser

# parameters
PAGE_START = 1
PAGE_END = 2

# XPAGE is a placeholder for the page number
URL = "https://www.realestate.com.au/buy/property-house-with-3-bedrooms-in-rangeville,+qld+4350%3b+south+toowoomba,+qld+4350%3b+centenary+heights,+qld+4350%3b+harristown,+qld+4350%3b+kearneys+spring,+qld+4350%3b+newtown,+qld+4350%3b+darling+heights,+qld+4350%3b+mount+lofty,+qld+4350%3b+middle+ridge,+qld+4350%3b+east+toowoomba,+qld+4350%3b+wilsonton+heights,+qld+4350%3b+wilsonton,+qld+4350%3b+glenvale,+qld+4350%3b+rockville,+qld+4350%3b+westbrook,+qld+4350/list-XPAGE?includeSurrounding=false&misc=ex-under-contract&activeSort=list-date"

# get this manually from the browser. go to the website, right click, inspect, network, click on the first request
# go to headers, on the request portion, copy the cookie
COOKIE = "INSERT_COOKIE_HERE"
USER_AGENT = "INSERT_USER_AGENT_HERE"


async def fetch(client, url, limiter, queue):
    headers = {
        "cookie": COOKIE,
        "user-agent": USER_AGENT,
    }

    async with limiter:
        res = await client.get(url, headers=headers)

        if len(res.text) < 2000:
            print("Error: Page not loaded properly")
            return None

        await queue.put(res.text)


def get_base_url(url):
    parsed_url = urlparse(url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    return base_url


def get_current_url(page):
    return URL.replace("XPAGE", str(page))


def process_html(html):
    data = []
    tree = HTMLParser(html)

    ul_el = tree.css_first("ul.tiered-results.tiered-results--exact")
    if ul_el:
        li_els = ul_el.css("li")
        for li in li_els:
            agent = li.css_first("p.agent__name")
            agent_text = agent.text() if agent else None

            address = li.css_first("h2")
            address_text = address.text() if address else None

            if agent_text is None and address_text is None:
                continue

            price = li.css_first("div.residential-card__price")
            price_text = price.text() if price else None

            link_tag = li.css_first("h2").css_first("a") if li.css_first("h2") else None
            link = link_tag.attributes["href"] if link_tag else None

            data.append(
                {
                    "agent": agent_text,
                    "address": address_text,
                    "price": price_text,
                    "link": get_base_url(URL) + link if link else None,
                }
            )

    return data


async def process_queue(queue):
    output_file = Path("output.json")
    output_data = []

    while True:
        html = await queue.get()
        if html is None:
            break

        data = process_html(html)
        output_data.extend(data)

        # Append to the JSON file
        with output_file.open("w") as f:
            json.dump(output_data, f, indent=4)

        queue.task_done()


async def main():
    rate_limit = AsyncLimiter(1, 3)  # requests per second
    queue = asyncio.Queue()
    async with httpx.AsyncClient() as client:
        tasks = [fetch(client, get_current_url(page), rate_limit, queue) for page in range(PAGE_START, PAGE_END + 1)]
        consumer_task = asyncio.create_task(process_queue(queue))

        await asyncio.gather(*tasks)

        # Signal the consumer to stop
        await queue.put(None)
        await consumer_task


if __name__ == "__main__":
    asyncio.run(main())
