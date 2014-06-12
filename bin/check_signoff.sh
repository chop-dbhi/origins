#!/bin/sh

env | sort

sudo apt-get update -qq
sudo apt-get install -qq python-yaml

git remote add upstream git://github.com/cbmi/origins.git

UPSTREAM=master
DIRNAME=$( cd "$( dirname "$0" )" && pwd )

if [ "$TRAVIS_PULL_REQUEST" != false ]; then
    UPSTREAM=$TRAVIS_BRANCH;
fi;

git fetch --append --no-tags upstream refs/heads/$UPSTREAM:refs/remotes/upstream/$UPSTREAM

/usr/bin/python "$DIRNAME/check_signoff.py"
