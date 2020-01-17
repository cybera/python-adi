# python-adi

A Python package for interacting with ADI via Python scripts and/or Jupyter notebooks

## Environment variables

This package needs to know where a valid ADI host is and have a valid api key. These
can be set as environment variables:

```bash
export ADI_API_HOST=your_api_host
export ADI_API_KEY=your_api_key
```

or, in python (before you `import adi`):

```python
import os
os.environ['ADI_API_HOST'] = your_api_host
os.environ['ADI_API_KEY'] = your_api_key
```

## Usage

If you've already set environment variables as indicated above, you can
just rely on the defaults:

```python
from adi import Connection
client = Connection()
client.organization.set_default('your-org')

# list datasets
adi.dataset.list()

# get a dataset
df = adi.dataset.get('some-dataset')
```

You can also specify a host and api key when initializing the `Connection`:

```python
from adi import Connection
client = Connection('http://your-host', 'your-api-key')
client.organization.set_default('your-org')

# list datasets
client.dataset.list()

# get a dataset
df = client.dataset.get('some-dataset')
```

## Other operations

The python API is split into 3 major high level namespaces: `dataset`, `transformation`, and `organization`. The main ones you'll be working with are the first two.

Uploading a dataset:

```python
client.dataset.upload('dataset-name', '/path/to/dataset.csv')
```

Defining a computed dataset (via a non-reusable transformation):

```python
client.dataset.define('computed-name', '/path/to/transformation.py')
```

where 'transformation.py' would be expected to have something like the following:

```python
df = dataset_input('dataset-name')

def transform():
  return df.head()
```

This transformation assumes the existence of 'dataset-name'.

If we wanted to define a *reusable* transformation, we do something very similar:

```python
client.transformation.define('reusable-name', '/path/to/reusable.py', inputs=['dataset-name'])
```

We could even use the same file as above. The main difference is to use this transformation, we have to tell it what *actual* datasets to map to `dataset-name`:

```python
client.dataset.define('computed-name-2', template='reusable-name', inputs={ 'dataset-name': 'some-dataset'})
```

In this case, we expect `some-dataset` to exist, but we could have used that transformation on any other existing dataset as well.

## Development setup

The following will get you into a bash shell where you can test out changes
to the package. By default, it's connected to the adi_default network, which
assumes that you've got ADI running locally. We'll probably want to make this
configurable later.

```bash
docker-compose run dev
```

The project directory will be available in /usr/src/pkg, so you can do a local
pip install via:

```bash
pip install -e /usr/src/pkg
```

That should allow you to:

```python
import adi
```

and work with whatever the most up to date code is in your project folder.
Keep in mind that you'll want to restart python and reimport your module to
test out changes to the code.

## Running tests

```bash
./run-tests.sh
```

This runs a python 3 development image, does a directory-based install of the current adi
python package, and then runs *main.py* under *./test/*.

Resulting dataset output will show up in *./test/data/output*.
