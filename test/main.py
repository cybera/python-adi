import os
import pandas as pd
from adi.dev.transformation import Transformation, transformation

def test_loader(datamap):
  if datamap["store"] == "test-local":
    path = os.path.join("/usr/src/pkg/test/data", datamap["value"])
  else:
    raise Exception("Don't know how to retrieve data that isn't in a test-local store")

  return pd.read_csv(path)

def test_writer(df, datamap):
  if datamap["store"] == "test-local":
    path = os.path.join("/usr/src/pkg/test/data/output", datamap["value"])
  else:
    raise Exception("Don't know how to write data that isn't in a test-local store")

  df.to_csv(path)

class TestTransformation(Transformation):
  def __init__(self, transform_func, loader={}, writer=None):
    super().__init__(transform_func, { "default": test_loader }, test_writer)

@transformation(TestTransformation)
def first10(iris):
  return iris.head(10)

print("Transformations found:\n")
keys = list(globals().keys())
for key in keys:
  if isinstance(globals()[key], Transformation):
    print(f"- {key}")

print("")

iris = pd.read_csv("/usr/src/pkg/test/data/iris.csv")
print(first10(iris))

first10.run({
  "input": {
    "iris": {
      "value": "iris.csv",
      "store": "test-local",
      "format": "csv"
    }
  },
  "output": {
    "value": "iris-first10.csv",
    "store": "test-local",
    "format": "csv"
  }
})
