# Intentional OLAP

Master:  ![Master](https://travis-ci.com/w4bo/experiments-assess.svg?token=eCxgQzWEteuAmE58GzVG&branch=master)
Develop: ![Develop](https://travis-ci.com/w4bo/experiments-assess.svg?token=eCxgQzWEteuAmE58GzVG&branch=develop)

# Running this project

Downloading the code, installing Java 14 and checking if the tests pass

    git clone git@github.com:w4bo/experiments-assess.git
    cd experiments-assess
    curl -sL https://github.com/shyiko/jabba/raw/master/install.sh | bash && . ~/.jabba/jabba.sh
    jabba install openjdk@1.14.0
    jabba use openjdk@1.14.0
    cd intentional
    cd src/main/python 
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    deactivate
    cd -
    git pull; ./gradlew --info

## Describe

Deploy the web application on Tomcat

    git pull; ./gradlew clean war; rm -r "C:\Program Files\Apache Software Foundation\Tomcat 9.0_Tomcat9-8083\webapps\IOLAP"; cp build/libs/IOLAP.war "C:\Program Files\Apache Software Foundation\Tomcat 9.0_Tomcat9-8083\webapps"

Running the scalability experiments

    git checkout resources/describe/time.csv; git pull; rm resources/describe/time.csv; ./gradlew runDescribe --info

## Assess

Running the scalability experiments

    git checkout resources/assess/time.csv; git pull; rm resources/assess/time.csv; ./gradlew runAssess  --info
