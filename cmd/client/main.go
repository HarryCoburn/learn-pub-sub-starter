package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// Connecting
	connectStr := "amqp://guest:guest@localhost:5672/"
	fmt.Println("Starting Peril client...")
	connection, err := amqp.Dial(connectStr)
	if err != nil {
		log.Fatal("Could not make connection")
	}
	defer connection.Close()
	fmt.Println("Connection successful")

	name, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal("Could not set username through ClientWelcome")
	}
	queueName := routing.PauseKey + "." + name
	pubsub.DeclareAndBind(connection, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.Transient)

	// Watch for interrupt
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Printf("Closing connection and shutting down. Goodbye.")

}
