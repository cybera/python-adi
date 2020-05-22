import setuptools

with open("README.md", "r") as fh:
  long_description = fh.read()

setuptools.setup(
  name="adi",
  version="0.0.1",
  author="Cybera",
  author_email="datascience@cybera.ca",
  description="Python API for interacting with the ADI platform",
  long_description=long_description,
  long_description_content_type="text/markdown",
  url="https://github.com/cybera/python-adi",
  packages=setuptools.find_packages(),
  classifiers=[
    "Programming Language :: Python :: 3",
    "Operating System :: OS Independent",
  ],
  python_requires='>=3.6',
  install_requires=[
    'pandas',
    'requests',
    'python-magic',
    'httpx'
  ]
)