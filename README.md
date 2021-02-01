# Kafka Schema Registry

## About

This has samples of kafka producers and consumers with schema registry.

This is developed using java 8 on Mac.

## Setup Kafka With Schema Registry
### Docker
* Install docker from ```https://www.docker.com/get-started```
* Start docker from applications
* Check docker status by running below commands in Terminal
  ```aidl
    docker --version
    docker-compose --version
    docker run hello-world
  ```
  Docker will download hello-world container and run it.
* Create docker-compose.yml file with below contents
  ```aidl
    version: '2'
    
    services:
      # this is our kafka cluster.
      kafka-cluster:
        image: landoop/fast-data-dev:cp3.3.0
        environment:
          ADV_HOST: 127.0.0.1         # Change to 192.168.99.100 if using Docker Toolbox
          RUNTESTS: 0                 # Disable Running tests so the cluster starts faster
          FORWARDLOGS: 0              # Disable running 5 file source connectors that bring application logs into Kafka topics
          SAMPLEDATA: 0               # Do not create sea_vessel_position_reports, nyc_yellow_taxi_trip_data, reddit_posts topics with sample Avro records.
        ports:
          - 2181:2181                 # Zookeeper
          - 3030:3030                 # Landoop UI
          - 8081-8083:8081-8083       # REST Proxy, Schema Registry, Kafka Connect ports
          - 9581-9585:9581-9585       # JMX Ports
          - 9092:9092                 # Kafka Broker
  ``` 
* Go to the directory where docker-compose.yml is created and run below command to start kafka cluster with schema registry
  ```aidl
    docker-compose up
  ``` 
* Open Landoop Web UI ```localhost:3030``` in browser.

  ![](src/test/resources/images/landoop-home.png)
  
* Enter to Schema Registry and set Compatibility Level to full
  
  ![](src/test/resources/images/schema-registry-compatibility.png)

* Create new schema by clicking on New. Make sure that name of the value and key schema is <topic-name>-value <topic-name>-key

  ![](src/test/resources/images/new-schema.png)
 
### Confluent

* Follow the instructions from below linka dn start confluent control centre
  
  ```aidl
    https://docs.confluent.io/platform/current/quickstart/ce-quickstart.html#ce-quickstart
  ```
  
  ![](src/test/resources/images/confluent-control-center.png)
  
* Open the Control center web interface ```localhost:9021```, create topic and set topic schema

  ![](src/test/resources/images/topic-creation.png)   
  
  ![](src/test/resources/images/schema-versions.png)    