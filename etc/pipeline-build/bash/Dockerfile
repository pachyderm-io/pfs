FROM ubuntu:18.04

WORKDIR /app
ADD build.sh .
ADD run.sh .
RUN apt-get -y update
RUN apt-get -y upgrade
RUN apt-get -y install gawk make jq parallel wget curl sed
