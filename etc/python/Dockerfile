FROM python:3.6-slim
LABEL maintainer="albert.cui@pachyderm.io"

RUN apt-get update && apt-get install -y curl
RUN pip3 install grpcio==1.38.0 grpcio-tools==1.38.0

ADD run.sh /bin
ENTRYPOINT ["/bin/run.sh"]
WORKDIR /work