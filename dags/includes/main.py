import re
import json
import asyncio
import uuid
import traceback
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from openai import OpenAI
from kafka import KafkaProducer
from includes import prompts                                                                                              # Folder path based on Docker Container
from config.config import configuration                                                                                   # Folder path based on Docker Container

SBR_WS_CDP = 'wss://'+configuration.get("BD_USERNAME")+':'+configuration.get("BD_PASSWORD")+'@brd.superproxy.io:9222'
client = OpenAI(api_key=configuration.get("OPEN_API_KEY"))



def extract_pictures(soup):
    print("Extracting Pictures")
    photos = []
    for div in soup.find_all('div', id=re.compile("^bp-PhotoArea__photoGridRow-")):
        for sub_div in div.find_all('div', {"id": re.compile("^MB-image-card-")}):
            photo = sub_div.find("img").get("src")
            photos.append(photo)
    return photos

def extract_key_details_details(html):
    print("Extracting Key Details")
    command = prompts.key_details_command(html)

    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{
            "role": "user",
            "content": command
        }]
    )

    result = response.choices[0].message.content
    json_data = json.loads(result or "")
    return json_data

def extract_listing_agent_details(html):
    print("Extracting Listing Agent Details")
    command = prompts.listing_agent_command(html)

    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{
            "role": "user",
            "content": command
        }]
    )

    result = response.choices[0].message.content
    json_data = json.loads(result or "")
    return json_data

def extract_redfin_agent_details(html):
    print("Extracting Redfin Agent Details")
    command = prompts.redfin_agent_command(html)

    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{
            "role": "user",
            "content": command
        }]
    )

    result = response.choices[0].message.content
    json_data = json.loads(result or "")
    return json_data

def extract_public_info_details(html):
    print("Extracting Property Details")
    command = prompts.public_info_command(html)

    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{
            "role": "user",
            "content": command
        }]
    )

    result = response.choices[0].message.content
    json_data = json.loads(result or "")
    return json_data


async def run(pw, producer):
    print('Connecting to Scraping Browser')
    browser = await pw.chromium.connect_over_cdp(SBR_WS_CDP)
    context = await browser.new_context()
    page = await context.new_page()
    try:
        print(f'Connected! Navigating to {configuration.get("BASE_URL")}')
        await page.goto(configuration.get("BASE_URL"))
        await page.wait_for_timeout(1000)
        await page.wait_for_load_state("load")

        await page.fill("input[placeholder='City, Address, School, Agent, ZIP']", configuration.get("LOCATION"))
        await page.keyboard.press("Enter")
        print('Searching...')
        await page.wait_for_timeout(1000)
        await page.wait_for_load_state("domcontentloaded")


        content = await page.inner_html("div[class='HomeCardsContainer flex flex-wrap']", timeout=120000)
        soup = BeautifulSoup(content, "html.parser")

        for idx, div in enumerate(soup.find_all('div', id=re.compile("^MapHomeCard_"))):
            print("--------")
            print(idx+1)

            listing = {
                "address": div.find("div", {"class": re.compile("^bp-Homecard__Address")}).text,
                "price": div.find("span", {"class": re.compile("^bp-Homecard__Price")}).text.replace("$", ""),
                "beds": div.find("span", {"class": re.compile("^bp-Homecard__Stats--beds")}).text,
                "baths": div.find("span", {"class": re.compile("^bp-Homecard__Stats--baths")}).text,
                "link": configuration.get("BASE_URL") + div.find("a")["href"]
            }

            # New Browser
            print('Navigating to Listing page: ' + listing["link"])
            browser2 = await pw.chromium.connect_over_cdp(SBR_WS_CDP)
            context2 = await browser2.new_context()
            page2 = await context2.new_page()
            await page2.wait_for_timeout(1000)
            await page2.goto(listing["link"], wait_until="domcontentloaded", timeout=120000)
            await page2.wait_for_timeout(4000)

            # Handle Photos: Some listings have pictures and others don't
            try:
                await page2.click("div[id='photoPreviewButton']", timeout=60000)
                await page2.wait_for_load_state("domcontentloaded")
                await page2.wait_for_selector("div[class='bp-PhotoArea bp-PhotoAreaGrid']", timeout=60000)
                photos_content = await page2.inner_html("div[class='bp-PhotoArea bp-PhotoAreaGrid']")
                photos_soup = BeautifulSoup(photos_content, "html.parser")
                listing['photos'] = list(filter(None, extract_pictures(photos_soup)))
                await page2.click("button[class='bp-Button bp-CloseButton bp-Button__type--primary bp-Button__icon-only']")
                await page2.wait_for_load_state("domcontentloaded", timeout=60000)
            except Exception:
                print("No Pictures")
                listing['photos'] = list()

            # Handle Property Details: Separate content out as the token count when call OpenAI API was too high
            key_details_content = await page2.inner_html("div[class='keyDetailsList']", timeout=120000)
            listing_agent_content = await page2.inner_html("div[class='agent-info-section']", timeout=120000)
            redfin_agent_content = await page2.inner_html("div[id='askAnAgent-collapsible']", timeout=120000)
            public_info_content = await page2.inner_html("div[id='publicFactsAndZoning-collapsible']", timeout=120000)

            key_details_soup = BeautifulSoup(key_details_content, "html.parser")
            listing_agent_soup = BeautifulSoup(listing_agent_content, "html.parser")
            redfin_agent_soup = BeautifulSoup(redfin_agent_content, "html.parser")
            public_info_soup = BeautifulSoup(public_info_content, "html.parser")

            listing.update(extract_key_details_details(key_details_soup))
            listing.update(extract_listing_agent_details(listing_agent_soup))
            listing.update(extract_redfin_agent_details(redfin_agent_soup))
            listing.update(extract_public_info_details(public_info_soup))

            # Upstream Transformation Listing
            listing['address'] = listing['address'].replace(",", "") if not None else ""
            listing['price'] = listing['price'].replace(",", "") if not None else ""
            listing['beds'] = listing['beds'].split(" ")[0] if not None else ""
            listing['baths'] = listing['baths'].split(" ")[0] if not None else ""
            listing['hoa_fee'] = listing['hoa_fee'].split(" ")[0].replace("$", "").replace("N/A", "").replace("n/a", "") if not None else ""
            listing['buyers_agent_fee'] = listing['buyers_agent_fee'].split(" ")[0].replace("%", "") if not None else ""
            listing['finished_sqft'] = listing['finished_sqft'].replace(",", "").replace("—", "") if not None else ""
            listing['unfinished_sqft'] = listing['unfinished_sqft'].replace(",", "").replace("—", "") if not None else ""
            listing['total_sqft'] = listing['total_sqft'].replace(",", "").replace("—", "") if not None else ""
            listing['stories'] = listing['stories'].replace("—", "") if not None else ""
            listing['style'] = listing['style'].replace("—", "") if not None else ""
            listing['year_built'] = listing['year_built'].replace("—", "") if not None else ""
            listing['year_renovated'] = listing['year_renovated'].replace("—", "") if not None else ""
            listing['county'] = listing['county'].replace("—", "") if not None else ""
            listing['APN'] = listing['APN'].replace(" ", "").replace("-", "").replace("—", "") if not None else ""

            # Upstream Final Checks
            listing['listing_agent_contact'] = listing['listing_agent_contact'] if "@" in listing['listing_agent_contact'] else ""
            listing['APN'] = listing['APN'] if listing['APN'] else str(uuid.uuid4())
            listing['county'] = listing['county'] if listing['county'] else "Fake County"
            print(json.dumps(listing, indent=2))

            # Send data to Kafka
            print("Sending Data to Kafka")
            producer.send(configuration.get("TOPIC"), json.dumps(listing).encode('utf-8'))

            # Close Browser
            await browser2.close()

    except Exception:
        print(traceback.format_exc())
    finally:
        await browser.close()



async def main():
    producer = KafkaProducer(bootstrap_servers=["kafka:9092"])
    async with async_playwright() as playwright:
        await run(playwright, producer)

def ingestion_start():
    asyncio.run(main())