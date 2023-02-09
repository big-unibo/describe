#!/bin/bash
set -exo

git pull

if [ -f .env ]; then
  set -a
  source ./.env
  set +a
fi

./gradlew clean war
rm -rf "${TOMCAT_PATH}\{ARTIFACT}" || true
cp "build/libs/${ARTIFACT}.war" "${TOMCAT_PATH}"
echo "Done."