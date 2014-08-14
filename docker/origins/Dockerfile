FROM dockerfile/ubuntu

MAINTAINER Byron Ruth <b@devel.io>

# Install dependencies
RUN apt-get -qq update
RUN apt-get -qq install -y python3 python3-pip

# Install Origins from source (for now)
RUN pip3 -q install "https://github.com/cbmi/origins/tarball/master#egg=origins"

# Allow for incoming connections
ENV ORIGINS_HOST 0.0.0.0

CMD ["python3", "-m", "origins.service"]
