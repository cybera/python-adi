import os
import pandas as pd
from adi.dev.transformation import Transformation, transformation, dataset

import pytest

TEST_DATA_ROOT = "/usr/src/pkg/test/data"

def mock_loader(datamap, variant="imported"):
  if datamap["storage"] == "test-local":
    path = os.path.join(TEST_DATA_ROOT, datamap["value"][variant])
  else:
    raise Exception("Don't know how to retrieve data that isn't in a test-local store")

  return pd.read_csv(path)

def mock_writer(df, datamap, variant="imported"):
  if datamap["storage"] == "test-local":
    path = os.path.join(TEST_DATA_ROOT, datamap["value"][variant])
  else:
    raise Exception("Don't know how to write data that isn't in a test-local store")

  df.to_csv(path)

class MockTransformation(Transformation):
  def __init__(self, transform_func, loader={}, writer=None, inputs={}):
    super().__init__(transform_func, { "default": mock_loader }, mock_writer, inputs=inputs)

  def record_storage_metadata(self, outputmap):
    outpath = os.path.join(TEST_DATA_ROOT, outputmap["value"]['imported'])
    self.metadata['bytes'] = os.stat(outpath).st_size

@pytest.fixture(scope="function", autouse=True)
def clean_environment():
  from glob import glob
  for path in glob(os.path.join(TEST_DATA_ROOT, 'output/*.csv')):
    os.unlink(path)

  yield # Execute the test

def test_basic():
  @transformation(MockTransformation)
  def first10(iris):
    return iris.head(10)

  print("Transformations found:\n")
  keys = list(locals().keys())

  assert('first10' in keys)
  assert(isinstance(locals()['first10'], Transformation))
  
  iris = pd.read_csv(os.path.join(TEST_DATA_ROOT, "iris.csv"))
  print(first10(iris))

  first10.run({
    "input": {
      "iris": {
        "value": {
          "original": "iris.csv",
          "imported": "iris.csv"
        },
        "storage": "test-local",
        "format": "csv"
      }
    },
    "output": {
      "value": {
        "imported": "output/iris-first10.imported.csv",
        "sample": "output/iris-first10.sample.csv"
      },
      "storage": "test-local",
      "format": "csv"
    }
  })
  assert(os.path.exists(os.path.join(TEST_DATA_ROOT, "output/iris-first10.imported.csv")))
  assert(os.path.exists(os.path.join(TEST_DATA_ROOT, "output/iris-first10.sample.csv")))

def test_nonreusable():
  @transformation(MockTransformation, inputs=dict(df = dataset('iris')))
  def iris_means(df):
    return (df.groupby(['species'])
              .mean())

  # simulate registering the transformation
  assert(iris_means.inputs == {
    'df': {
      'ref': 'iris',
      'reftype': 'dataset',
      'variant': 'imported'
    }
  })

  iris_means.run({
    "input": {
      "df": {
        "value": {
          "original": "iris.csv",
          "imported": "iris.csv"
        },
        "storage": "test-local",
        "format": "csv"
      }
    },
    "output": {
      "value": {
        "original": "output/iris-means.original.csv",
        "imported": "output/iris-means.imported.csv",
        "sample": "output/iris-means.sample.csv"
      },
      "storage": "test-local",
      "format": "csv"
    }
  })

  assert(os.path.exists(os.path.join(TEST_DATA_ROOT, "output/iris-means.imported.csv")))
  assert(os.path.exists(os.path.join(TEST_DATA_ROOT, "output/iris-means.sample.csv")))

  # should be around 246, but we mainly care that it's in the ballpark
  assert(iris_means.metadata['bytes'] > 0 and iris_means.metadata['bytes'] < 500)
  column_keys = set(iris_means.metadata['columns'][0].keys())
  assert(column_keys - { 'name', 'order', 'originalName', 'tags' } == set())
