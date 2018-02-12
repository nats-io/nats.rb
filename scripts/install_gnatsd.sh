#!/bin/sh

set -e

# check to see if gnatsd folder is empty

if [ ! "$(ls -A $HOME/gnatsd)" ]; then
  mkdir -p $HOME/gnatsd
  cd $HOME/gnatsd
  wget https://github.com/nats-io/gnatsd/releases/download/v1.0.4/gnatsd-v1.0.4-linux-amd64.zip -O gnatsd.zip
  unzip gnatsd.zip
  mv gnatsd-v1.0.4-linux-amd64/gnatsd .
else
  echo 'Using cached directory.';
fi
