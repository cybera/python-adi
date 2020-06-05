import httpx
import asyncio
from importlib.util import spec_from_file_location, module_from_spec
from tempfile import NamedTemporaryFile
from io import BytesIO
from magic import Magic
import requests
import pandas as pd

magic = Magic(mime_encoding=True)

def read_raw(url, chunksize=None):
  res = requests.get(url, stream=(chunksize != None))
  if res.status_code != 200:
    raise Exception(f"Failed to read dataset {url} got {res.status_code}")
  if chunksize:
    return res.iter_content(chunk_size=chunksize)
  else:
    return res.content

def read_csv(url, params={}, detectEncoding=False, chunksize=None):
  encoding = None
  if detectEncoding:
    # guess the encoding from up to the first 10MB of the file
    checkbytes = 1024*1024*10
    headers = {"Range": f"bytes=0-{checkbytes}"}
    res = requests.get(url, headers=headers)
    encoding = magic.from_buffer(res.content)

  params = { 'encoding': encoding, **params }

  if chunksize:
    params['chunksize'] = chunksize

  return pd.read_csv(url, **params)

def write_raw(data, url):
  res = requests.put(url, data)
  if res.status_code != 200 and res.status_code != 201:
    raise Exception("Failed to write dataset")

def write_csv(df, url):
  data = df.to_csv(index=False).encode('utf-8')
  write_raw(data, url)

async def adf_chunk_encoder(dfs):
  # We only want to write a header from the first chunk
  first_chunk = True

  async for chunk in dfs:
    encoded_chunk = chunk.to_csv(index=False, header=first_chunk).encode('utf-8')
    first_chunk = False
    yield encoded_chunk

async def write_csv_stream(dfs, url):
  data = adf_chunk_encoder(dfs)
  client = httpx.AsyncClient()
  res = await client.put(url, data=data)
  await client.aclose()
  if res.status_code != 200 and res.status_code != 201:
    raise Exception("Failed to write dataset")

async def write_raw_stream(data, url):
  client = httpx.AsyncClient()
  res = await client.put(url, data=data)
  await client.aclose()
  if res.status_code != 200 and res.status_code != 201:
    raise Exception("Failed to write dataset")

def cleanup_script_module(module):
  pass

def __metadata(url):
  res = requests.head(url)
  if res.status_code != 200:
    raise Exception(f"Failed to read metadata for dataset {url} got {res.status_code}")

  return res.headers

def bytes(url):
  metadata = __metadata(url)
  return int(metadata['Content-Length'])
