#!/bin/bash
set -exo

############################################################
# REMEMBER TO RUN THIS SCRIPT WITH bash AND NOT WITH sh
############################################################
find . -type f -iname "*.sh" -exec chmod +x {} \;

echo "Replacing .env.example with .env"
cp .env.example .env
echo "Replacing src/main/resources/config.example.yml src/main/resources/config.yml"
cp src/main/resources/config.example.yml src/main/resources/config.yml

P=$(pwd)
echo $P
sed -i "s+\!HOME\!+${P}+g" src/main/resources/config.yml
sed -i "s+\!HOME\!+${P}+g" .env
sed -i "s+\!HOME\!+${P}+g" start.sh

cd src/main/python
if [ -d venv ]; then
    echo "venv already exists"
else
    if [[ "$(python -V 2>&1)" == "Python 3" ]]; then
        echo "python found"
        python -m venv venv
    fi
    if [[ "$(python3 -V 2>&1)" =~ "Python 3" ]]; then
        echo "python3 found"
        python3 -m venv venv
    fi
fi

if [ -d venv/bin ]; then
    source venv/bin/activate
else
    source venv/Scripts/activate
fi

pip install -r requirements.txt
chmod -R 777 venv
cd -