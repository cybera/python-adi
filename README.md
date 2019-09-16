# python-adi

A Python package for interacting with ADI via Python scripts and/or Jupyter notebooks

## Environment variables

This package needs to know where your ADI host is and your access key. Either do the
following before running python:

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
