package main

import (
	"github.com/MarcoTomasRodriguez/kafka-mqtt-connector/sink"
	"github.com/MarcoTomasRodriguez/kafka-mqtt-connector/source"
	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// Set log formatter to text.
	log.SetFormatter(&log.TextFormatter{})

	log.Println("Welcome to Kafka-MQTT connector.")

	// Load the environment variables from .env.
	err := godotenv.Load()
	if err != nil {
		log.Warnf("Could not load .env file: %v.", err)
	}

	// signalChan listens to a syscall to interrupt the connector.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	// sourceErrorChan represents an unrecoverable error in the source.
	sourceErrorChan := make(chan error, 1)

	// sinkErrorChan represents an unrecoverable error in the sink.
	sinkErrorChan := make(chan error, 1)

	// Define your source mappings here ->
	sourceMapping := source.Mapping{}

	// Define your sink mapping here ->
	sinkMapping := sink.Mapping{}

	log.Infoln("Initializing source & sink...")

	// Execute source & sink in their own goroutines.
	go source.ExecuteSource(sourceMapping, sourceErrorChan)
	go sink.ExecuteSink(sinkMapping, sinkErrorChan)

	for {
		select {
		// In case of an error in the source, log it and retry.
		case err := <-sourceErrorChan:
			log.Errorf("Source: Error %v", err)
			go source.ExecuteSource(sourceMapping, sourceErrorChan)
		// In case of an error in the sink, log it and retry.
		case err := <-sinkErrorChan:
			log.Errorf("Sink: Error %v", err)
			go sink.ExecuteSink(sinkMapping, sinkErrorChan)
		// Exit the program.
		case <-signalChan:
			return
		}
	}
}
