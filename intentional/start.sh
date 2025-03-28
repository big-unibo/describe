#!/bin/bash
set -exo

if [ -f .env ]; then
  set -a
  source ./.env
  set +a
fi

./stop.sh
docker compose up --build -d
./wait-for-it.sh ${MYSQL_URL}:${MYSQL_PORT} --strict --timeout=420 -- echo "MySQL is up"

./gradlew --stacktrace --scan