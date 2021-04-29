#FROM python:3.9.4-alpine3.13
FROM amancevice/pandas:1.2.4-alpine
#FROM amancevice/pandas:1.2.4-slim

COPY dwh.py /.

RUN  chmod +x /dwh.py \
    && mkdir /data \
    && mkdir /result

WORKDIR /result

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["/dwh.py"]
