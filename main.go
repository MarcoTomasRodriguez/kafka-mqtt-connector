package main

import (
	"github.com/MarcoTomasRodriguez/kafka-mqtt-connector/events"
	"github.com/MarcoTomasRodriguez/kafka-mqtt-connector/sink"
	"github.com/MarcoTomasRodriguez/kafka-mqtt-connector/source"
	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// SetupSource starts the execution of the sourceMapping.
func SetupSource(sourceMapping source.Mapping, wg *sync.WaitGroup) {
	// Add this goroutine to wait group.
	wg.Add(1)

	go func() {
		// Remove goroutine from wait group upon termination.
		defer wg.Done()

		// signalChan listens to a syscall to interrupt the connector.
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

		// sourceChan is the single channel used to communicate with the executor.
		sourceChan := make(chan interface{}, 1)

		// Execute source in their own goroutine.
		go source.ExecuteSource(sourceMapping, sourceChan, wg)

		for {
			select {
			// Send to sourceChan the cancellation event when an os signal is triggered.
			case sig := <-signalChan:
				sourceChan <- events.CancelEvent{Signal: sig}
			// Listen to events from source.
			case sourceEvent := <- sourceChan:
				switch sourceEvent.(type) {
				case events.ErrorEvent:
					log.Errorf("Source: Error %v", sourceEvent)
					go source.ExecuteSource(sourceMapping, sourceChan, wg)
				case events.ExitEvent:
					log.Infoln("Source: Terminated")
					return
				}
			}
		}
	}()
}

// SetupSink starts the execution of the sinkMapping.
func SetupSink(sinkMapping sink.Mapping, wg *sync.WaitGroup) {
	// Add this goroutine to wait group.
	wg.Add(1)

	go func() {
		// Remove goroutine from wait group upon termination.
		defer wg.Done()

		// signalChan listens to a syscall to interrupt the connector.
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

		// sinkChan is the single channel used to communicate with the executor.
		sinkChan := make(chan interface{}, 1)

		// Execute sink in their own goroutine.
		go sink.ExecuteSink(sinkMapping, sinkChan, wg)

		for {
			select {
			// Send to sinkChan the cancellation event when an os signal is triggered.
			case sig := <-signalChan:
				sinkChan <- events.CancelEvent{Signal: sig}
			// Listen to the events from sink.
			case sinkEvent := <-sinkChan:
				switch sinkEvent.(type) {
				case events.ErrorEvent:
					log.Errorf("Sink: Error %v", sinkEvent)
					go sink.ExecuteSink(sinkMapping, sinkChan, wg)
				case events.ExitEvent:
					log.Infoln("Sink: Terminated")
					return
				}
			}
		}
	}()
}

func main() {
	// Set log formatter to text.
	log.SetFormatter(&log.TextFormatter{})

	log.Println("Welcome to Kafka-MQTT connector.")

	// Load the environment variables from .env.
	err := godotenv.Load()
	if err != nil {
		log.Warnf("Could not load .env file: %v.", err)
	}

	log.Infoln("Initializing source & sink...")

	// Create wait group.
	var wg sync.WaitGroup

	// Define your source mappings here ->
	sourceMapping := source.Mapping{}

	// Define your sink mapping here ->
	sinkMapping := sink.Mapping{}

	// Setup source and sink.
	SetupSource(sourceMapping, &wg)
	SetupSink(sinkMapping, &wg)

	// Wait for all goroutines to stop.
	wg.Wait()
}
