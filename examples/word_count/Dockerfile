FROM ubuntu:18.04

WORKDIR /go/src/app
COPY src/map.go .

ENV GOLANG_VERSION 1.15.4
ENV GOLANG_DOWNLOAD_URL https://golang.org/dl/go$GOLANG_VERSION.linux-amd64.tar.gz
ENV GOLANG_DOWNLOAD_SHA256 eb61005f0b932c93b424a3a4eaa67d72196c79129d9a3ea8578047683e2c80d5
RUN go get -d -v ./...
RUN go install -v ./...

CMD ["app"]
