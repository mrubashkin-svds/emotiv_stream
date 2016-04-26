# emotiv_stream
Stream processing of emotiv EKG data through Kafka Streaming - to graphite/grafana web server
![Alt text](README_Items/Architecture.png)

# Streaming data
Emotiv --> iOS --> Flume --> KafkaConsumer --> Kafka Streaming --> Carbon/Whisper/Graphite --> Grafana
![Alt text](README_Items/TeethClench.png)
