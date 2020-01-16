from types import FunctionType
import os
import pandas as pd
import requests

def csv_loader(tmpurl):
  # TODO: Use requests. Check if file in /tmp, write to that and read from the local path.
  path = os.path.join("sample_data", dataptr)
  return pd.read_csv(path)

def csv_writer(df, tmpurl):
  # TODO: Use requests. Should be able to write directly to the tmpurl.
  path = os.path.join("sample_data", dataptr)
  df.to_csv(path)

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