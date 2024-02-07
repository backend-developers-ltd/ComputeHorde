import sys
import requests

source = sys.argv[1]
target = sys.argv[2]

res = requests.get(source, stream=True)
with open(target, 'wb') as fp:
    raw_bytes = res.raw.read()
    fp.write(raw_bytes)
