''' Scraping Solids used for the Real-Estate project '''

import requests
import re
import datetime
from bs4 import BeautifulSoup
from datetime import datetime
import json

import gzip
import shutil

# from io import BytesIO

from realestate.common.helper_functions import json_zip_writer


from dagster import (
    Any,
    Int,
    solid,
    Field,
    String,
    OutputDefinition,
    composite_solid,
    FileHandle,
    LocalFileHandle,
    ExpectationResult,
    EventMetadataEntry,
    Output,
)
from .types_realestate import PropertyDataFrame, SearchCoordinate, JsonType


from dagster_aws.s3.solids import S3Coordinate

from realestate.common.solids_spark_delta import upload_to_s3
from realestate.common.solids_filehandle import json_to_gzip
from dagster.utils.temp_file import get_temp_file_name


@solid(
    description='''This will scrape immoscout without hitting the API (light scrape)''',
    config_schema={
        'immo24_main_url_en': Field(
            String,
            default_value='https://www.immoscout24.ch/en/',
            is_required=False,
            description=(
                '''Main URL to start the search with (propertyType unspecific).
            No API will be hit with this request. This is basic scraping.'''
            ),
        ),
        'immo24_search_url_en': Field(
            String,
            default_value='https://www.immoscout24.ch/en/real-estate/',
            is_required=False,
            description=(
                '''Main URL to start the search with (propertyType specific).
            No API will be hit with this request. This is basic scraping.'''
            ),
        ),
    },
)
def list_props_immo24(context, searchCriteria: SearchCoordinate) -> PropertyDataFrame:

    # propertyType : str, rentOrBuys : str, radius : int, cities : str)
    listPropertyIds = []

    # find max page
    url = (
        context.solid_config['immo24_search_url_en']
        + searchCriteria['rentOrBuy']
        + '/city-'
        + searchCriteria['city']
        + '?r='
        + str(searchCriteria['radius'])
        + '&map=1'  # only property with prices (otherwise mapping id to price later on does not map)
        + ''
    )
    context.log.info('Search url: {}'.format(url))
    html = requests.get(url)
    soup = BeautifulSoup(html.text, "html.parser")
    buttons = soup.findAll('button')
    p = []
    for item in buttons:
        if len(item.text) <= 3 & len(item.text) != 0:
            p.append(item.text)
    if p:
        lastPage = int(p.pop())
    else:
        lastPage = 1
    context.log.info('Count of Pages found: {}'.format(lastPage))

    ids = []
    propertyPrice = []
    for i in range(1, lastPage + 1):
        url = (
            context.solid_config['immo24_main_url_en']
            + searchCriteria['propertyType']
            + '/'
            + searchCriteria['rentOrBuy']
            + '/city-'
            + searchCriteria['city']
            + '?pn='  # pn= page site
            + str(i)
            + '&r='
            + str(searchCriteria['radius'])
            + '&se=16'  # se=16 means most recent first
            + '&map=1'  # only property with prices (otherwise mapping id to price later on does not map)
        )
        context.log.debug('Range search url: {}'.format(url))

        html = requests.get(url)
        soup = BeautifulSoup(html.text, "html.parser")
        links = soup.findAll('a', href=True)
        hrefs = [item['href'] for item in links]
        hrefs_filtered = [href for href in hrefs if href.startswith('/en/d')]
        ids += [re.findall('\d+', item)[0] for item in hrefs_filtered]

        # get normalized price without REST-call
        h3 = soup.findAll('h3')
        for h in h3:
            text = h.getText()
            if 'CHF' in text or 'EUR' in text:
                start = text.find('CHF') + 4
                end = text.find('.â\x80\x94')

                price = text[start:end]

                # remove all characters except numbers --> equals price
                price = re.sub('\D', '', price)
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
                'id': id,  # name = parition
                'fingerprint': str(id) + '-' + str(last_normalized_price),
                'is_prefix': False,
                'rentOrBuy': searchCriteria['rentOrBuy'],
                'city': searchCriteria['city'],
                'propertyType': searchCriteria['propertyType'],
                'radius': searchCriteria['radius'],
                'last_normalized_price': str(last_normalized_price),
            }
        )

    return listPropertyIds


@solid(
    description='Downloads full dataset (JSON) of property against API',
    # output_defs=[OutputDefinition(JsonType)],
    required_resource_keys={"file_cache"},
    config_schema={
        'immo24_api_en': Field(
            String,
            default_value='https://rest-api.immoscout24.ch/v4/en/properties/',
            is_required=False,
            description=(
                '''Main URL to start the search with (propertyType unspecific).
            No API will be hit with this request. This is basic scraping.'''
            ),
        )
    },
)
def cache_properies_from_rest_api(
    context, properties: PropertyDataFrame, target_key: String
) -> FileHandle:

    property_list = []
    date = datetime.today().strftime('%y%m%d')
    date_time = datetime.now().strftime("%y%m%d_%H%M%S")
    for p in properties:

        # Is it possible to do a range instead of each seperately?
        json_prop = requests.get(context.solid_config['immo24_api_en'] + p['id']).json()

        # add metadata if flat, house, detatched-house, etc.
        json_prop['propertyDetails']['propertyType'] = p['propertyType']
        json_prop['propertyDetails']['isBuyRent'] = p['rentOrBuy']

        # add metadata from search
        json_prop['propertyDetails']['propertyId'] = p['id']
        json_prop['propertyDetails']['searchCity'] = p['city']
        json_prop['propertyDetails']['searchRadius'] = p['radius']
        json_prop['propertyDetails']['searchDate'] = date
        json_prop['propertyDetails']['searchDateTime'] = date_time

        property_list.append(json_prop)

    filename = (
        property_list[0]['propertyDetails']['searchDate']
        + '_'
        + property_list[0]['propertyDetails']['searchCity']
        + '_'
        + property_list[0]['propertyDetails']['isBuyRent']
        + '_'
        + str(property_list[0]['propertyDetails']['searchRadius'])
        + '_'
        + property_list[0]['propertyDetails']['propertyType']
        + '.gz'
    )
    target_key = target_key + '/' + filename

    '''caching to file
    '''
    file_cache = context.resources.file_cache
    target_file_handle = file_cache.get_file_handle(target_key)

    if file_cache.overwrite or not file_cache.has_file_object(target_key):
        json_zip_writer(property_list, target_key)
        context.log.info("File handle written at : {}".format(target_file_handle.path_desc))
    else:
        context.log.info("File {} already present in cache".format(target_file_handle.path_desc))

    yield ExpectationResult(
        success=file_cache.has_file_object(target_key),
        label="file_handle_exists",
        metadata_entries=[
            EventMetadataEntry.path(path=target_file_handle.path_desc, label=target_key)
        ],
    )
    yield Output(target_file_handle)


@solid(description='Get price from a property via API Call')
def _get_normalized_price(context, id: Int):
    api_url = 'https://rest-api.immoscout24.ch/v4/en/properties/'
    propApi = requests.get(api_url + id).json()

    if 'propertyDetails' in propApi:
        if 'normalizedPrice' in propApi['propertyDetails']:
            last_checked_price = propApi['propertyDetails']['normalizedPrice']
        else:
            last_checked_price = None
    else:
        last_checked_price = None

    return last_checked_price
