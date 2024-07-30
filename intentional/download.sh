#!/bin/bash
set -exo
cd resources
curl -L -o foodmart-mysql.sql https://big.csr.unibo.it/projects/nosql-datasets/foodmart-mysql.sql
curl -L -o covid_weekly-mysql.sql https://big.csr.unibo.it/projects/nosql-datasets/covid_weekly-mysql.sql
cd ..
