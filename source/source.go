package source

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
)

// Topic is the MQTT Topic that will trigger the resolver.
type Topic struct {
	Topic string
	QoS byte
}

// Resolver is the action to be executed when the MQTT Topic is triggered.
type Resolver = func(message mqtt.Message) *kafka.Message

// Mapping joins the topic with their resolver.
type Mapping = map[Topic]Resolver

// handleResolver executes the resolver.
func handleResolver(resolver Resolver, kafkaProducerChan chan *kafka.Message) func(mqtt.Client, mqtt.Message) {
	return func(client mqtt.Client, message mqtt.Message) {
		kafkaMessage := resolver(message)
		log.Debugf("Source: Forwarding MQTT message %s from topic %s to Kafka topic %s.",
			message.Payload(), message.Topic(), *kafkaMessage.TopicPartition.Topic)
		kafkaProducerChan <- kafkaMessage
	}
}

// ExecuteSource configures all source mappings and gives them an environment of execution.
func ExecuteSource(mapping Mapping, sourceErrorChan chan error) {
	// Exit source if no mappings are defined.
	if len(mapping) == 0 {
		log.Warnln("Source: No mappings are defined. Exiting source.")
		return
	}

	// Create a Kafka producer using some of the environment variables.
	// Please see https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html
	kafkaProducer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
		"client.id": os.Getenv("SOURCE_CLIENT_ID"),
		"acks": os.Getenv("KAFKA_ACKS"),
		"security.protocol": "SASL_SSL",
		"sasl.mechanism": "PLAIN",
		"sasl.username": os.Getenv("KAFKA_SASL_USERNAME"),
		"sasl.password": os.Getenv("KAFKA_SASL_PASSWORD"),
		"batch.size": os.Getenv("KAFKA_BATCH_SIZE"),
	})
	if err != nil {
		sourceErrorChan <- err
		return
	}

	// kafkaProducerChan is a channel in which every message received is sent to the Kafka cluster.
	kafkaProducerChan := kafkaProducer.ProduceChannel()

	// Convert MQTT_BROKER_PORT to int.
	mqttPort, err := strconv.Atoi(os.Getenv("MQTT_BROKER_PORT"))
	if err != nil {
		sourceErrorChan <- err
		return
	}

	// Setup MQTT client.
	mqttOpts := mqtt.NewClientOptions()
	mqttOpts.AddBroker(fmt.Sprintf("tcp://%s:%d", os.Getenv("MQTT_BROKER_HOST"), mqttPort))
	mqttOpts.SetClientID(os.Getenv("SOURCE_CLIENT_ID"))

	mqttOpts.OnConnect = func(client mqtt.Client) {
		log.Infof("Source MQTT: Connected.")
	}

	mqttOpts.OnConnectionLost = func(client mqtt.Client, err error) {
		log.Errorf("Source MQTT: Disconnected with error: %v. Reconnecting.", err)
		client.Connect()
	}

	// Connect to MQTT broker.
	mqttClient := mqtt.NewClient(mqttOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		sourceErrorChan <- token.Error()
		return
	}

	// Configure all mappings.
	for key, resolver := range mapping {
		token := mqttClient.Subscribe(key.Topic, key.QoS, handleResolver(resolver, kafkaProducerChan))
		if token.Wait() && token.Error() != nil {
			kafkaProducer.Flush(3000)
			sourceErrorChan <- token.Error()
			return
		}
		log.Infof("Source MQTT: Subscribed to topic: %s with QoS: %v.", key.Topic, key.QoS)
	}
}