# $BOONTADATA_DOCKER_REGISTRY/boontadata-paas/pyclientbase
#
# VERSION   0.1

FROM continuumio/anaconda3

MAINTAINER boontadata <contact@boontadata.io>

RUN apt-get update && \
	apt-get install -y \
		git \
		build-essential \
		vim \
		telnet

RUN pip install pydocumentdb

ENTRYPOINT ["init"]
