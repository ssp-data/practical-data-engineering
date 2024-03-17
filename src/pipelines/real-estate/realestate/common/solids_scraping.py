""" Scraping Ops used for the Real-Estate project """

from typing import Any, Generator
import requests
import re
import datetime
from bs4 import BeautifulSoup
from datetime import datetime

from contextlib import closing
import io

# from io import BytesIO

from realestate.common.helper_functions import json_zip_writer


from dagster import (
    Int,
    op,
    Field,
    String,
    LocalFileHandle,
    Out,
    Output,
)


from .types_realestate import PropertyDataFrame, SearchCoordinate, JsonType


@op(
    description="""This will scrape immoscout without hitting the API (light scrape)""",
    config_schema={
        "immo24_main_url_en": Field(
            String,
            default_value="https://www.immoscout24.ch/en/",
            is_required=False,
            description=(
                """Main URL to start the search with (propertyType unspecific).
            No API will be hit with this request. This is basic scraping."""
            ),
        ),
        "immo24_search_url_en": Field(
            String,
            default_value="https://www.immoscout24.ch/en/real-estate/",
            is_required=False,
            description=(
                """Main URL to start the search with (propertyType specific).
            No API will be hit with this request. This is basic scraping."""
            ),
        ),
    },
    required_resource_keys={"fs_io_manager"},
    out=Out(io_manager_key="fs_io_manager"),

)
def list_props_immo24(context, searchCriteria: SearchCoordinate) -> PropertyDataFrame:
    # propertyType : str, rentOrBuys : str, radius : int, cities : str)
    listPropertyIds = []

    # find max page
    url = (
        context.op_config["immo24_search_url_en"]
        + searchCriteria["rentOrBuy"]
        + "/city-"
        + searchCriteria["city"]
        + "?r="
        + str(searchCriteria["radius"])
        + "&map=1"  # only property with prices (otherwise mapping id to price later on does not map)
        + ""
    )
    context.log.info("Search url: {}".format(url))
    html = requests.get(url)
    soup = BeautifulSoup(html.text, "html.parser")
    buttons = soup.findAll("a")
    p = []
    for item in buttons:
        if len(item.text) <= 3 & len(item.text) != 0:
            p.append(item.text)
    if p:
        lastPage = int(p.pop())
    else:
        lastPage = 1
    context.log.info("Count of Pages found: {}".format(lastPage))

    ids = []
    propertyPrice = []
    for i in range(1, lastPage + 1):
        url = (
            context.op_config["immo24_main_url_en"]
            + searchCriteria["propertyType"]
            + "/"
            + searchCriteria["rentOrBuy"]
            + "/city-"
            + searchCriteria["city"]
            + "?pn="  # pn= page site
            + str(i)
            + "&r="
            + str(searchCriteria["radius"])
            + "&se=16"  # se=16 means most recent first
            + "&map=1"  # only property with prices (otherwise mapping id to price later on does not map)
        )
        context.log.debug("Range search url: {}".format(url))

        html = requests.get(url)
        soup = BeautifulSoup(html.text, "html.parser")
        links = soup.findAll("a", href=True)
        hrefs = [item["href"] for item in links]
        hrefs_filtered = [href for href in hrefs if href.startswith("/" + searchCriteria['rentOrBuy'] + "/")]
        ids += [re.findall("\d+", item)[0] for item in hrefs_filtered]

        # get normalized price without REST-call
        h3 = soup.findAll("span")
        for h in h3:
            text = h.getText()
            if "CHF" in text or "EUR" in text:
                start = text.find("CHF") + 4
                end = text.find(".â\x80\x94")

                price = text[start:end]

                # remove all characters except numbers --> equals price
                price = re.sub("\D", "", price)
                propertyPrice.append(price)

    # merge all ids and prices to dict
    dictPropPrice = {}
    for j in range(len(ids)):
        dictPropPrice[ids[j]] = propertyPrice[j]

    propDict = {}
    # loop through dict
    for id in dictPropPrice:
        last_normalized_price = dictPropPrice[id]

        listPropertyIds.append(
            {
                "id": id,  # name = parition
                "fingerprint": str(id) + "-" + str(last_normalized_price),
                "is_prefix": False,
                "rentOrBuy": searchCriteria["rentOrBuy"],
                "city": searchCriteria["city"],
                "propertyType": searchCriteria["propertyType"],
                "radius": searchCriteria["radius"],
                "last_normalized_price": str(last_normalized_price),
            }
        )

    return listPropertyIds


@op(
    description="Downloads and cache full datasets (JSON) as gzip to avoid re-downloading same properties against API. If file already exists, it will not re-downloaded again.",
    required_resource_keys={"file_manager"},
    config_schema={
        "immo24_api_en": Field(
            String,
            default_value="https://api.immoscout24.ch/listings/listing/",
            is_required=False,
            description=(
                """Main URL to start the search with (propertyType unspecific).
            No API will be hit with this request. This is basic scraping."""
            ),
        )
    },
    out={"local_file_handle": Out(LocalFileHandle, io_manager_key="fs_io_manager")},
)
def cache_properies_from_rest_api(
    context, properties: PropertyDataFrame
) -> Generator[Any, None, None]:
    property_list = []
    date = datetime.today().strftime("%y%m%d")
    date_time = datetime.now().strftime("%y%m%d_%H%M%S")

    for p in properties:

        context.log.debug(f"Request sent to {context.op_config['immo24_api_en'] + p['id']}")
        # Is it possible to do a range instead of each seperately?
        json_prop = requests.get(context.op_config["immo24_api_en"] + p["id"]).json()

        # add metadata if flat, house, detatched-house, etc.
        if "propertyDetails" not in json_prop:
            json_prop["propertyDetails"] = {}
        json_prop["propertyDetails"]["propertyType"] = p["propertyType"]
        json_prop["propertyDetails"]["isBuyRent"] = p["rentOrBuy"]

        # add metadata from search
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

    # Create a BytesIO object to hold the gzipped data temporarily
    with closing(io.BytesIO()) as temp_file_obj:
        json_zip_writer(property_list, temp_file_obj)
        
        # Reset the file pointer to the beginning after writing
        temp_file_obj.seek(0)
        
        # Use LocalFileManager to write the gzipped data to a file
        file_handle = local_file_manager.write(temp_file_obj, mode="wb", ext="gz")

        context.log.info(f"File handle written at : {file_handle.path}")
        yield Output(value=file_handle, output_name="local_file_handle")



@op(description="Get price from a property via API Call")
def _get_normalized_price(context, id: Int):
    api_url = "https://rest-api.immoscout24.ch/v4/en/properties/"
    propApi = requests.get(api_url + id).json()

    if "propertyDetails" in propApi:
        if "normalizedPrice" in propApi["propertyDetails"]:
            last_checked_price = propApi["propertyDetails"]["normalizedPrice"]
        else:
            last_checked_price = None
    else:
        last_checked_price = None

    return last_checked_price
