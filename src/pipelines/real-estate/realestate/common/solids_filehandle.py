import json
import gzip

from realestate.common.types_realestate import JsonType


def json_to_gzip(json_data: JsonType):
    json_bytes = json_data.encode('utf-8')
    return gzip.compress(json_bytes)


# @dg.op(description='Zipping json file')
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
