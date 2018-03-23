#!/bin/sh

set -e

if [ ! "$(ls -A $HOME/gnatsd)" ]; then
  mkdir -p $HOME/gnatsd
  cd $HOME/gnatsd
  wget https://github.com/nats-io/gnatsd/releases/download/v1.1.0/gnatsd-v1.1.0-linux-amd64.zip -O gnatsd.zip
  unzip gnatsd.zip
  mv gnatsd-v1.1.0-linux-amd64/gnatsd .
else
  echo 'Using cached directory.';
fi
