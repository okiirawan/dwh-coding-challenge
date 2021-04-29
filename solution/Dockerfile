FROM amancevice/pandas:1.2.4-alpine

COPY dwh.py /.

RUN  chmod +x /dwh.py \
    && mkdir /data \
    && mkdir /result

WORKDIR /result

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["/dwh.py"]
