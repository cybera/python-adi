from types import FunctionType
import os
import pandas as pd
import requests
import hashlib
from . import storage

SAMPLE_SIZE=100

def default_loader(datamap, variant='imported'):
  if datamap['storage'] == 'swift-tempurl':
    if datamap['format'] == 'csv':
      return storage.read_csv(datamap['value'][variant])
    else:
      return storage.read_raw(datamap['value'][variant])
  else:
    raise Exception(f"Don't know how to deal with storage: {datamap['storage']}")

def default_writer(data, datamap, variant='imported'):
  if datamap['storage'] == 'swift-tempurl':
    # TODO: Should probably figure out from the return type and not the passed in format
    if datamap['format'] == 'csv':
      storage.write_csv(data, datamap['value'][variant])
    else:
      storage.write_raw(data, datamap['value'][variant])
  else:
    raise Exception(f"Don't know how to deal with storage: {datamap['storage']}")

class Transformation:
  def __init__(self, transform_func, loader={ 'default': default_loader }, writer=default_writer, inputs={}):
    self.transform_func = transform_func
    self.loader = loader
    self.writer = writer
    self.inputs = inputs
    self.metadata = dict()

  def __call__(self, *args, **kwargs):
    return self.transform_func(*args, **kwargs)

  def load(self, input_params):
    print(f"loading {input_params}")

    loaded_params = {}

    for k,v in input_params.items():
      # Get default values set when creating the transformation. All of these
      # can potentially be overridden by ADI.
      if k in self.inputs and isinstance(self.inputs[k], dict):
        datamap = { **self.inputs[k], **v }
      else:
        datamap = v

      if k in self.loader:
        print(f"Specialized loader for {k}")
        loaded_params[k] = self.loader[k](datamap)
      else:
        loaded_params[k] = self.loader['default'](datamap)
  
    return loaded_params

  def transform(self, inputs):
    return self.transform_func(**inputs)

  def output(self, result, outputptr):
    print(f"output: {outputptr}")
    self.writer(result, outputptr)

  def sample(self, result):
    sample_data = ""
    if type(result) is pd.DataFrame:
      sample_size = min(result.shape[0], SAMPLE_SIZE)
      sample_data = result.sample(sample_size)
    return sample_data
    
  def output_sample(self, result, outputptr):
    sample_data = self.sample(result)
    self.writer(sample_data, outputptr, variant='sample')

  def record_result_metadata(self, result):
    if type(result) is pd.DataFrame:
      column_types = result.dtypes
      columns = [dict(name=name,
                      originalName=name,
                      tags=[convert_type(column_types[name])],
                      order=i+1) for i, name in enumerate(result.columns)]
      self.metadata['columns'] = columns
      self.metadata['type'] = 'csv'
    else:
      self.metadata['type'] = 'document'

  def record_storage_metadata(self, outputptr):
    self.metadata['bytes'] = storage.bytes(outputptr['value']['imported'])

  def run(self, params):
    loaded = self.load(params['input'])
    results = self.transform(loaded)
    self.output(results, params['output'])
    self.output_sample(results, params['output'])
    self.record_result_metadata(results)
    self.record_storage_metadata(params['output'])

def transformation(TransformationClass=Transformation, loader={ 'default': default_loader }, inputs={}):
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
    return Transformation(TransformationClass, inputs=inputs)
  else:
    def wrap(transform_func):
      t = TransformationClass(transform_func, loader, inputs=inputs)
      return t
    return wrap

def dataset(name, org=None, variant="imported"):
  if org:
    name = f"{org}:{name}"

  return { 'ref': name, 'reftype': 'dataset', 'variant': variant }

def convert_type(pd_type):
  if pd_type == 'object':
    return 'String'
  elif pd_type == 'int64':
    return 'Integer'
  elif pd_type == 'float64':
    return 'Float'
  elif pd_type == 'bool':
    return 'Boolean'
  else:
    # String is a good default if nothing more specific can be found
    return 'String'