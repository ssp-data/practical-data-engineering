""" Scraping Ops used for the Real-Estate project """

from typing import Any, Generator
import requests
import re
from bs4 import BeautifulSoup
from datetime import datetime
import json

from contextlib import closing
import io

from realestate.common.helper_functions import json_zip_writer

import dagster as dg

from .types_realestate import PropertyDataFrame, SearchCoordinate, JsonType

def _extract_listings_from_json_ld(soup: BeautifulSoup, rent_or_buy: str) -> list[dict]:
    """Extract property IDs and prices from JSON-LD structured data in the page.
    Returns list of dicts with 'id' and 'price' keys."""
    listings = []
    ld_scripts = soup.find_all("script", {"type": "application/ld+json"})
    for script in ld_scripts:
        if not script.string:
            continue
        try:
            data = json.loads(script.string)
        except json.JSONDecodeError:
            continue
        if data.get("@type") != "Product":
            continue
        # Extract price
        price = ""
        offers = data.get("offers", [])
        if isinstance(offers, list) and offers:
            price = str(offers[0].get("price", ""))
        elif isinstance(offers, dict):
            price = str(offers.get("price", ""))

        # ID not in JSON-LD, will be matched from <a> hrefs
        listings.append({"name": data.get("name", ""), "price": price})
    return listings


def _extract_property_ids_from_links(soup: BeautifulSoup, rent_or_buy: str) -> list[str]:
    """Extract property IDs from <a> hrefs. Handles both relative and absolute URLs."""
    ids = []
    links = soup.find_all("a", href=True)
    for link in links:
        href = link["href"]
        # Match both "/buy/ID" and "https://www.immoscout24.ch/buy/ID"
        match = re.search(rf"/{rent_or_buy}/(\d+)", href)
        if match:
            prop_id = match.group(1)
            if prop_id not in ids:
                ids.append(prop_id)
    return ids


@dg.op(
    description="""Scrapes immoscout24.ch search results using JSON-LD structured data and link parsing.""",
    config_schema={
        "immo24_search_url_en": dg.Field(
            str,
            default_value="https://www.immoscout24.ch/en/real-estate/",
            is_required=False,
            description="Base search URL for immoscout24.",
        ),
    },
    out=dg.Out(io_manager_key="fs_io_manager"),
)
def list_props_immo24(context, searchCriteria: SearchCoordinate) -> PropertyDataFrame:
    rent_or_buy = searchCriteria["rentOrBuy"]
    city = searchCriteria["city"]
    radius = searchCriteria["radius"]
    property_type = searchCriteria["propertyType"]

    url = (
        context.op_config["immo24_search_url_en"]
        + rent_or_buy
        + "/city-"
        + city
        + "?r="
        + str(radius)
        + "&map=1"
    )
    context.log.info(f"Search url: {url}")

    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    # Extract property IDs from links
    ids = _extract_property_ids_from_links(soup, rent_or_buy)
    context.log.info(f"Found {len(ids)} property IDs from links: {ids}")

    # Extract prices from JSON-LD structured data
    json_ld_listings = _extract_listings_from_json_ld(soup, rent_or_buy)
    context.log.info(f"Found {len(json_ld_listings)} listings from JSON-LD")

    # Build ID→price mapping: match by position (JSON-LD and links appear in same order)
    dict_prop_price = {}
    for i, prop_id in enumerate(ids):
        if i < len(json_ld_listings):
            dict_prop_price[prop_id] = json_ld_listings[i]["price"]
        else:
            dict_prop_price[prop_id] = ""

    # Fall back to <span> price parsing if JSON-LD didn't work
    if not json_ld_listings and ids:
        context.log.info("No JSON-LD data found, falling back to span price parsing")
        prices = []
        for span in soup.find_all("span"):
            text = span.get_text()
            if "CHF" in text or "EUR" in text:
                price = re.sub(r"\D", "", text)
                if price:
                    prices.append(price)
        for i, prop_id in enumerate(ids):
            dict_prop_price[prop_id] = prices[i] if i < len(prices) else ""

    # Handle pagination: check for page links
    page_links = soup.find_all("a", href=re.compile(r"[?&]pn=\d+"))
    page_numbers = set()
    for link in page_links:
        match = re.search(r"[?&]pn=(\d+)", link["href"])
        if match:
            page_numbers.add(int(match.group(1)))
    last_page = max(page_numbers) if page_numbers else 1
    context.log.info(f"Pages found: {last_page}")

    # Scrape remaining pages (page 1 already done)
    for page in range(2, last_page + 1):
        page_url = url + f"&pn={page}"
        context.log.debug(f"Fetching page {page}: {page_url}")
        page_response = requests.get(page_url)
        page_soup = BeautifulSoup(page_response.text, "html.parser")

        page_ids = _extract_property_ids_from_links(page_soup, rent_or_buy)
        page_listings = _extract_listings_from_json_ld(page_soup, rent_or_buy)

        for i, prop_id in enumerate(page_ids):
            if prop_id not in dict_prop_price:
                price = page_listings[i]["price"] if i < len(page_listings) else ""
                dict_prop_price[prop_id] = price

    # Build result list
    result = []
    for prop_id, price in dict_prop_price.items():
        result.append(
            {
                "id": prop_id,
                "fingerprint": f"{prop_id}-{price}",
                "is_prefix": False,
                "rentOrBuy": rent_or_buy,
                "city": city,
                "propertyType": property_type,
                "radius": radius,
                "last_normalized_price": str(price),
            }
        )

    context.log.info(f"Total properties found: {len(result)}")
    return result


@dg.op(
    description="Downloads and cache full datasets (JSON) as gzip to avoid re-downloading same properties against API. If file already exists, it will not re-downloaded again.",
    required_resource_keys={"file_manager"},
    config_schema={
        "immo24_api_en": dg.Field(
            str,
            default_value="https://api.immoscout24.ch/listings/listing/",
            is_required=False,
            description=(
                """Main URL to start the search with (propertyType unspecific).
            No API will be hit with this request. This is basic scraping."""
            ),
        )
    },
    out={"local_file_handle": dg.Out(dg.LocalFileHandle, io_manager_key="fs_io_manager")},
)
def cache_properies_from_rest_api(
    context, properties: PropertyDataFrame
) -> Generator[Any, None, None]:
    property_list = []
    date = datetime.today().strftime("%y%m%d")
    date_time = datetime.now().strftime("%y%m%d_%H%M%S")

    for p in properties:

        context.log.debug(f"Request sent to {context.op_config['immo24_api_en'] + p['id']}")
        json_prop = requests.get(context.op_config["immo24_api_en"] + p["id"]).json()

        if "propertyDetails" not in json_prop:
            json_prop["propertyDetails"] = {}
        json_prop["propertyDetails"]["propertyType"] = p["propertyType"]
        json_prop["propertyDetails"]["isBuyRent"] = p["rentOrBuy"]

        json_prop["propertyDetails"]["propertyId"] = p["id"]
        json_prop["propertyDetails"]["searchCity"] = p["city"]
        json_prop["propertyDetails"]["searchRadius"] = p["radius"]
        json_prop["propertyDetails"]["searchDate"] = date
        json_prop["propertyDetails"]["searchDateTime"] = date_time

        property_list.append(json_prop)

    filename = (
        property_list[0]["propertyDetails"]["searchDate"]
        + "_"
        + property_list[0]["propertyDetails"]["searchCity"]
        + "_"
        + property_list[0]["propertyDetails"]["isBuyRent"]
        + "_"
        + str(property_list[0]["propertyDetails"]["searchRadius"])
        + "_"
        + property_list[0]["propertyDetails"]["propertyType"]
        + ".gz"
    )

    """caching to file
    """
    local_file_manager = context.resources.file_manager

    with closing(io.BytesIO()) as temp_file_obj:
        json_zip_writer(property_list, temp_file_obj)

        temp_file_obj.seek(0)

        file_handle = local_file_manager.write(temp_file_obj, mode="wb", ext="gz")

        context.log.info(f"File handle written at : {file_handle.path}")
        yield dg.Output(value=file_handle, output_name="local_file_handle")
