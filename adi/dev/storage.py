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
  return len(data)

def write_csv(df, url):
  data = df.to_csv(index=False).encode('utf-8')
  return write_raw(data, url)

def read_script_module(url):
  raw = read_raw(url)

  # Write to a temporary file so we can load the script
  temp_script = NamedTemporaryFile(delete=False, suffix='.py')
  temp_script.write(raw)
  temp_script.close()

  # Load the script as a module
  transform_spec = spec_from_file_location("transform", temp_script.name)
  transform_module = module_from_spec(transform_spec)

  return transform_module

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
