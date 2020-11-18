'''Compress and decompress a JSON object'''

import zlib
import gzip
import json, base64


def json_zip_writer(j, target_key):
    f = gzip.open(target_key, 'wb')
    f.write(json.dumps(j).encode('utf-8'))
    f.close()


def json_zip(j):

    # return base64.b64encode(zlib.compress(json.dumps(j).encode('utf-8'))).decode('ascii')
    return zlib.compress(json.dumps(j).encode('utf-8'))


def json_unzip(j, insist=True):

    try:
        # j = zlib.decompress(base64.b64decode(j))
        j = zlib.decompress(j)
    except:
        raise RuntimeError("Could not decode/unzip the contents")

    try:
        j = json.loads(j)
    except:
        raise RuntimeError("Could interpret the unzipped contents")

    return j
