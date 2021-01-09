package sink

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
)

// Message stores the information of a MQTT to be later published.
type Message struct {
	Topic string
	Qos byte
	Retained bool
	Payload []byte
}

// Topic is the Kafka Topic that will trigger the resolver.
type Topic = string

// Resolver is the action to be executed when the topic is triggered.
type Resolver = func(message *kafka.Message) Message

// Mapping joins the topic with their resolver.
type Mapping = map[Topic]Resolver

// handleResolver triggers the defined action given a Kafka message.
func handleResolver(message *kafka.Message, mapping Mapping, mqttClient mqtt.Client) {
	kafkaTopic := *message.TopicPartition.Topic
	if resolver, found := mapping[kafkaTopic]; found {
		mqttMessage := resolver(message)
		log.Debugf("Sink: Forwarding Kafka message %s with key %s from topic %s to MQTT topic %s.",
			string(message.Value), string(message.Key), *message.TopicPartition.Topic, mqttMessage.Topic)
		mqttClient.Publish(mqttMessage.Topic, mqttMessage.Qos, mqttMessage.Retained, mqttMessage.Payload)
	}
}

// ExecuteSink configures all sink mappings and gives them an environment of execution.
func ExecuteSink(mapping Mapping, sinkErrorChan chan error) {
	// Exit sink if no mappings are defined.
	if len(mapping) == 0 {
		log.Warnln("Sink: No mappings are defined. Exiting sink.")
		return
	}

	// Create a Kafka consumer using some of the environment variables.
	// Please see https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html
	kafkaConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
		"client.id": os.Getenv("SINK_CLIENT_ID"),
		"group.id": os.Getenv("KAFKA_GROUP_ID"),
		"auto.offset.reset": "latest",
		"auto.commit.interval.ms": os.Getenv("KAFKA_AUTO_COMMIT_INTERVAL"),
		"security.protocol": "SASL_SSL",
		"sasl.mechanism": "PLAIN",
		"sasl.username": os.Getenv("KAFKA_SASL_USERNAME"),
		"sasl.password": os.Getenv("KAFKA_SASL_PASSWORD"),
	})
	if err != nil {
		sinkErrorChan <- err
		return
	}

	// Convert MQTT_BROKER_PORT to int.
	mqttPort, err := strconv.Atoi(os.Getenv("MQTT_BROKER_PORT"))
	if err != nil {
		sinkErrorChan <- err
		return
	}

	// Setup MQTT client.
	mqttOpts := mqtt.NewClientOptions()
	mqttOpts.AddBroker(fmt.Sprintf("tcp://%s:%d", os.Getenv("MQTT_BROKER_HOST"), mqttPort))
	mqttOpts.SetClientID(os.Getenv("SINK_CLIENT_ID"))

	mqttOpts.OnConnect = func(client mqtt.Client) {
		log.Infof("Sink MQTT: Connected.")
	}

	mqttOpts.OnConnectionLost = func(client mqtt.Client, err error) {
		log.Errorf("Sink MQTT: Disconnected with error: %v. Reconnecting.", err)
	}

	// Connect to MQTT broker.
	mqttClient := mqtt.NewClient(mqttOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		sinkErrorChan <- token.Error()
		return
	}

	// Get to all topics defined in the mapping.
	kafkaTopics := make([]string, 0, len(mapping))
	for topic := range mapping {
		kafkaTopics = append(kafkaTopics, topic)
	}

	// Subscribe to all topics defined in the mapping.
	err = kafkaConsumer.SubscribeTopics(kafkaTopics, nil)
	if err != nil {
		sinkErrorChan <- err
		return
	}

	// Execute mapping's resolver.
	for {
		ev := kafkaConsumer.Poll(0)
		switch event := ev.(type) {
		case *kafka.Message:
			handleResolver(event, mapping, mqttClient)
		case kafka.Error:
			sinkErrorChan <- event
			return
		default:
		}
	}
}