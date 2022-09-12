#!/bin/bash
set -exo
cp .env.example .env
cp src/main/resources/config.example.yml src/main/resources/config.yml
P=$(pwd)
sed -i "s+\!HOME\!+${P}+g" src/main/resources/config.yml
sed -i "s+\!HOME\!+${P}+g" start.sh