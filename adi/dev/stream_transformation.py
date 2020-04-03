from .transformation import Transformation, default_analyzer
import asyncio

from .transformation import Transformation, default_analyzer
from . import storage

def create_stream_loader(chunksize):
  def stream_loader(datamap, variant='imported'):
    if datamap['storage'] == 'swift-tempurl':
      if datamap['format'] == 'csv':
        return storage.read_csv(datamap['value'][variant], chunksize=chunksize)
      else:
        return storage.read_raw(datamap['value'][variant])
    else:
      raise Exception(f"Don't know how to deal with storage: {datamap['storage']}")
  return stream_loader

async def default_stream_analyzer(dfs, metadata):
  recorded = False
  async for df in dfs:
    if not recorded:
      default_analyzer(df, metadata)
      recorded = True

class StreamTransformation(Transformation):
  def __init__(self, *args, **kwargs):
    streamargs = { 'analyzer': default_stream_analyzer, **kwargs }
    super().__init__(*args, **streamargs)

  def load(self, input_params):
    loaded_params = super().load(input_params)
    return ParamStream(loaded_params)

  def output_sample(self, results, outputptr):
    async def async_output_sample(results):
      async for result in results:
        yield self.sample(result)
        
    return self.writer(async_output_sample(results), outputptr, variant='sample')

  def run(self, params):
    param_chunks = self.load(params['input'])
    results = (self.transform(chunk) for chunk in param_chunks)
    (r1, r2, r3) = fork_iter(results, 3)
    async def tasks():
      await asyncio.gather(
        self.output(r1, params['output']),
        self.output_sample(r2, params['output']),
        self.record_result_metadata(r3)
      )
    asyncio.run(tasks())
    self.record_storage_metadata(params['output'])

class ParamStream:
  def __init__(self, params):
    self.params = params
    self.__first_iteration_complete = False

  def __iter__(self):
    return self

  def __next__(self):
    internal_iter = None
    internal_iter_stopped = False

    param_chunk = dict()
    for k,v in self.params.items():
      try:
        param_chunk[k] = next(v)
        # If we have absolutely no internal iterators, we will want to stop
        # after the first iteration.
        if internal_iter and internal_iter != k:
          raise Exception("Param stream should only have one iterable param")
        internal_iter = k
      except TypeError:
        # Not all parameters will be iterable. But the transformation function 
        # will still need them
        param_chunk[k] = v
      except StopIteration:
        internal_iter_stopped = True

    if (self.__first_iteration_complete and not internal_iter) or internal_iter_stopped:
      raise StopIteration

    self.__first_iteration_complete = True

    return param_chunk

def fork_iter(iter, num):
  from itertools import tee
  iters = list(tee(iter, num))
  return [__pausable(i) for i in iters]

async def __pausable(iter):
  for i in iter:
    await asyncio.sleep(0)
    yield i