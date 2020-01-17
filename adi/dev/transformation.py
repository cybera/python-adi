from types import FunctionType
import os
import pandas as pd
import requests
import hashlib

def resource_hash(datamap):
  return hashlib.sha256(datamap['value'].encode('utf-8')).hexdigest()

def default_loader(datamap):
  if datamap['storage'] == 'swift-tempurl':
    ref = resource_hash(datamap)
    tmppath = f"/tmp/adi/{ref}"
    urlstream = requests.get(datamap['value'], stream=True)
    # TODO: Check if tmppath exists already before downloading to it?
    with open(tmppath, 'wb') as fd:
      for chunk in urlstream.iter_content(chunk_size):
        fd.write(chunk)
    
    if datamap['format'] == 'csv':
      return pd.read_csv(path)
    else:
      raise Exception(f"Don't know how to deal with format: {datamap['format']}")
  else:
    raise Exception(f"Don't know how to deal with storage: {datamap['storage']}")

def default_writer(df, datamap):
  if datamap['storage'] == 'swift-tempurl':
    ref = resource_hash(datamap)
    tmppath = f"/tmp/adi/{ref}"

    if datamap['format'] == 'csv':
      # TODO: Check if tmppath exists already before writing to it?
      df.to_csv(tmppath)
      requests.put(datamap['value'], data=open(tmppath, 'rb'))
    else:
      raise Exception(f"Don't know how to deal with format: {datamap['format']}")
  else:
    raise Exception(f"Don't know how to deal with storage: {datamap['storage']}")

class Transformation:
  def __init__(self, transform_func, loader={ 'default': default_loader }, writer=default_writer):
    self.transform_func = transform_func
    self.loader = loader
    self.writer = writer

  def __call__(self, *args, **kwargs):
    return self.transform_func(*args, **kwargs)

  def load(self, input_params):
    print(f"loading {input_params}")

    loaded_params = {}

    for k,v in input_params.items():
      if k in self.loader:
        print(f"Specialized loader for {k}")
        loaded_params[k] = self.loader[k](v)
      else:
        loaded_params[k] = self.loader['default'](v)
  
    return loaded_params

  def transform(self, inputs):
    return self.transform_func(**inputs)

  def output(self, result, outputptr):
    print(f"outputting: {result}")
    self.writer(result, outputptr)

  def run(self, params):
    loaded = self.load(params['input'])
    results = self.transform(loaded)
    self.output(results, params['output'])
    return results

def transformation(TransformationClass=Transformation, loader={}):
  # Note that to test for functions, the normal recommended route is using
  # callable(suspected_function). But in our case, we're using __call__ in
  # the Transformation class itself, so when we really want to test for a
  # primitive non-class function (which... neat! is actually kind of an object)
  # we need to be more specific. And the reason we have to do all of this is
  # how much decorator definitions change between zero arguments and some. If
  # we were okay forcing the user to use: @transform() instead of @transform
  # in a zero arg case, we could do away with it. But this particular function
  # shouldn't have to change that often, so it's a small bit of weirdness to
  # make things feel slicker for our wonderful data scientists.
  if isinstance(TransformationClass, FunctionType):
    print("calling decorator with no args")
    return Transformation(TransformationClass)
  else:
    def wrap(transform_func):
      t = TransformationClass(transform_func, loader)
      return t
    return wrap