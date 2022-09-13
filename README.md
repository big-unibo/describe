# Intentional OLAP: Describe

[![build](https://github.com/big-unibo/describe/actions/workflows/build.yml/badge.svg)](https://github.com/big-unibo/describe/actions/workflows/build.yml)
![Coverage](.github/badges/jacoco.svg)

## Research papers

Please refer/cite to the following research paper:

- Francia, Matteo, et al. "Enhancing cubes with models to describe multidimensional data." **Information Systems Frontiers** (2022): 31-48. DOI: https://doi.org/10.1007/s10796-021-10147-3
- Chédin, Antoine, et al. "The tell-tale cube." **European Conference on Advances in Databases and Information Systems**. Springer, Cham, 2020. DOI: https://doi.org/10.1007/978-3-030-54832-2\_16

## Running the experiments

This repository allows the user to:
1. download the necessary datasets;
2. bring up a Docker container with MySQL;
3. load the datasets into MySQL;
4. run the tests.

Running the experiments requires the following software to be installed:
- Docker
- Java 14
- Python 3.6.9

Once the software is installed, execute the following code to run the tests.

    cd intentional
    chmod +x *.sh
    ./init.sh
    ./build.sh
    ./download.sh
    ./start.sh
    ./stop.sh
