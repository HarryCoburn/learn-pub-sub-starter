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

	moveChan, err := connection.Channel()
	if err != nil {
		log.Fatal("Could not open channel: ", err)
	}

	name, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal("Could not set username through ClientWelcome")
	}
	queueName := routing.PauseKey + "." + name
	armyQueueName := routing.ArmyMovesPrefix + "." + name
	newGameState := gamelogic.NewGameState(name)
	pubsub.SubscribeJSON(connection, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.Transient, handlerPause(newGameState))
	pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, armyQueueName, routing.ArmyMovesPrefix+".*", pubsub.Transient, handlerMove(newGameState))

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		if input[0] == "quit" {
			gamelogic.PrintQuit()
			break
		}
		if input[0] == "spawn" {
			err = newGameState.CommandSpawn(input)
			if err != nil {
				fmt.Printf("Error: %v", err)
				continue
			}
			fmt.Println("Spawned")
			continue
		}
		if input[0] == "move" {
			move, err := newGameState.CommandMove(input)
			if err != nil {
				fmt.Printf("Error: %v", err)
				continue
			}
			err = pubsub.PublishJSON(moveChan, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+name, move)
			fmt.Println("Move published successfully")

			continue
		}
		if input[0] == "status" {
			newGameState.CommandStatus()
			continue
		}
		if input[0] == "help" {
			gamelogic.PrintClientHelp()
			continue
		}
		if input[0] == "spam" {
			fmt.Println("Spamming not allowed yet!")
			continue
		}

		fmt.Println("I do not understand the command.")
	}

	// Watch for interrupt
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Printf("Closing connection and shutting down. Goodbye.")

}
