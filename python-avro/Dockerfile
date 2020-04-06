FROM python:3.6-alpine

RUN sed -i -e 's/v3\.4/edge/g' /etc/apk/repositories  && \
    apk --no-cache add alpine-sdk librdkafka-dev && \
    pip install --no-cache-dir confluent-kafka[avro]==1.3.0 && \
	pip install --upgrade avro-python3==1.8.2 && \
    apk del alpine-sdk && \
    rm -rf /root/cache/*