## Kafka-MQTT Connector

### .env

```dotenv
MQTT_BROKER_HOST=test.mosquitto.org
MQTT_BROKER_PORT=1883

KAFKA_BOOTSTRAP_SERVERS=...
KAFKA_SASL_USERNAME=...
KAFKA_SASL_PASSWORD=...
KAFKA_GROUP_ID=kafka-mqtt-connector
KAFKA_AUTO_COMMIT_INTERVAL=1000
KAFKA_BATCH_SIZE=1
KAFKA_ACKS=all

SOURCE_CLIENT_ID=source-client-id
SINK_CLIENT_ID=sink-client-id
```