import json
import gzip

from dagster import solid, FileHandle, Failure

# from realestate.common.solids_scraping import cache_file_from_input

from realestate.common.types_realestate import JsonType


def json_to_gzip(json_data: JsonType):
    # encoded = json_file.encode('utf-8')
    # j = json.dumps(json_data)
    json_bytes = json_data.encode('utf-8')

    return gzip.compress(json_bytes)


# @solid(description='Zipping json file')
# def json_to_gzip(context, json_path: FileHandle) -> FileHandle:

#     jfile = open(json_path, "r")
#     if jfile.mode == 'r':
#         json_data = json.dumps(jfile, indent=2)
#         encoded = json_data.encode('utf-8')
#         return cache_file_from_input(
#             inputData=gzip.compress(encoded),
#             file_key=FileHandle + '.gzip',
#         )
#     else:
#         raise Failure("Can't open json_path {path}".format(path=json_path))
