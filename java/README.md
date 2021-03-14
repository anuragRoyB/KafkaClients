# Overview

Produce messages to a Kafka cloud cluster using the Java Producer API.


# Documentation

build code: mvn clean install
run code: mvn exec:java -Dexec.mainClass="io.confluent.examples.clients.cloud.ProducerExample" -Dexec.args="C:\Work\Forgerock\Workspace\Kafka\java.config testfrim2 5"
arguments: config location, topic name, number of records
