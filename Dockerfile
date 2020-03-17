FROM python:3

RUN mkdir -p /usr/src/pkg
RUN mkdir -p /usr/src/app

WORKDIR /usr/src/app

RUN pip install --no-cache-dir requests pandas pytest

ENTRYPOINT [ "bash" ]
