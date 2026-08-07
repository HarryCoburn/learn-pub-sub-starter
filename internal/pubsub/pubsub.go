package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
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

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("failure declaring and binding: %w", err)
	}
	consumeChan, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failure consuming: %w", err)
	}
	go func() {
		var ackType AckType
		for msg := range consumeChan {
			var target T
			if err := json.Unmarshal(msg.Body, &target); err != nil {
				log.Println("failed to unmarshal message:", err)
				continue
			}
			ackType = handler(target)
			switch ackType {
			case Ack:
				fmt.Println("Received Ack")
				msg.Ack(false)
			case NackRequeue:
				fmt.Println("Received NackRequeue")
				msg.Nack(false, true)
			case NackDiscard:
				fmt.Println("Received NackDiscard")
				msg.Nack(false, false)

			}
		}
	}()
	return nil
}
