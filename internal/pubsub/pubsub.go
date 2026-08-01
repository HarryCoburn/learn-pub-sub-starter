package pubsub

import (
	"context"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonVal, err := json.Marshal(val)
	if err != nil {
		return err
	}
	ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        jsonVal,
	})
	return nil
}

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("Could not open channel: ", err)
	}
	newQueue := amqp.Queue{}
	if queueType == Durable {
		newQueue, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
	} else { // Assume transient
		newQueue, err = ch.QueueDeclare(queueName, false, true, true, false, nil)
	}
	if err != nil {
		log.Fatal("Could not make queue: ", err)
	}
	err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		log.Fatal("Could not bind queue to channel: ", err)
	}
	return ch, newQueue, nil
}
