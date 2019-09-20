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
adi = Connection()
adi.organization.set_default('your-org')

# list datasets
adi.list()

# get a dataset
df = adi.get('some-dataset')
```

You can also specify a host and api key when initializing the `Connection`:

```python
from adi import Connection
adi = Connection('http://your-host', 'your-api-key')
adi.organization.set_default('your-org')

# list datasets
adi.list()

# get a dataset
df = adi.get('some-dataset')
```

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
