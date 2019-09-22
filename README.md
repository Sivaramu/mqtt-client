# mqtt-client
MQTT Kafka Spring Boot

## Softwares
1. VerneMQ 1.9.0
2. Erlang/OTP 21.2+
2. Kafka 2.12
3. Spring Boot 2.1.8

### Setup

A simple and quick demo to test the throughput and latency between VerneMQ and Kafka

1. Setup VerneMQ in local. Erlang/OTP is pre-requisite and status page http://localhost:8888/status should be up and running after the setup.  
2. Use Eclipse Paho 1.2.1 mqttv3 library to publish/subscribe messages to VerneMQ
3. Setup Zookeeper and Kafka
4. Create a spring boot application using Spring Initializr with Web and Kakfa starters
5. Modify the subscriber to push messages to Kakfa using KafkaTemplate


### Run

Create a topic 
> bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic iot-devices && bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic iot-devices

Run Spring Boot
> mvn clean package

### Results

MQTT Publishers rate of of message push was a 12,500 messages per second in Ubuntu 19.03, Intel i5-4460 3.20GHz, 8GB, WDC WD10EZEX-21M. Kakfa had 100 MB/s ingestion rate when tested with ProducerPerformance tool and was able to consume the messages to the rate of 12,000 messages per second.

Both VerneMQ and Kafka seems to be able to ingest at much higher rates (several times) even on the local desktop. The configuration options must be tuned further for both VerneMQ and Kafka.



