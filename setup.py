import setuptools

with open("README.md", "r") as fh:
  long_description = fh.read()

setuptools.setup(
  name="synthi",
  version="0.0.1",
  author="Cybera",
  author_email="datascience@cybera.ca",
  description="Python API for interacting with the Synthi platform",
  long_description=long_description,
  long_description_content_type="text/markdown",
  url="https://github.com/cybera/python-synthi",
  packages=setuptools.find_packages(),
  classifiers=[
    "Programming Language :: Python :: 3",
    "Operating System :: OS Independent",
  ],
  python_requires='>=3.6',
  install_requires=[
    'pandas'
  ]
)
