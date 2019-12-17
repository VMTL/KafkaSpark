# Java Kafka - Spark streaming  

This is a sample of a Kafka - Spark streaming application for a near real-time news processing

## Tools:

Kafka
Spark
Cassandra

## Prerequisites:
Locally installed Kafka and Zookeeper (I am using pre-installed Zookeeper version in Kafka), Cassandra

## Runnig the application:
Run Zookeeper
```
%KAFKA_HOME%\bin\windows\zookeeper-server-start.bat %KAFKA_HOME%\config\zookeeper.properties
```
Run Kafka
```
%KAFKA_HOME%\bin\windows\kafka-server-start.bat %KAFKA_HOME%\config\server.properties
```
Run Cassandra
```
%CASSANDRA_HOME%\bin\cassandra.bat
```
Run Application
```
mvn clean compile exec:java -Dexec.mainClass="app.Application"
```