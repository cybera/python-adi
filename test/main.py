import os
import pandas as pd
from adi.dev.transformation import Transformation, transformation

def test_loader(dataptr):
  path = os.path.join("/usr/src/pkg/test/data", dataptr)
  return pd.read_csv(path)

def test_writer(df, dataptr):
  path = os.path.join("/usr/src/pkg/test/data/output", dataptr)
  df.to_csv(path)

class TestTransformation(Transformation):
  def __init__(self, transform_func, loader={}, default_loader=None, writer=None):
    super().__init__(transform_func, loader, test_loader, test_writer)

@transformation(TestTransformation)
def first10(iris):
  return iris.head(10)

iris = pd.read_csv("/usr/src/pkg/test/data/iris.csv")
print(first10(iris))

first10.run({
  "input": {
    "iris": "iris.csv"
  },
  "output": "iris-first10.csv"
})

keys = list(globals().keys())
for key in keys:
  if isinstance(globals()[key], Transformation):
    print(key)
