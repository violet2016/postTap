package communicator

import (
	"log"

	"github.com/streadway/amqp"
)

type MessageProcessor interface {
	Process(msg []byte) error
}
type Communicator interface {
	Send(queue string, msg []byte) error
	Receive(queue string) (<-chan amqp.Delivery, error)
}
type AmqpComm struct {
	conn *amqp.Connection
}

// Receive keep receive from the queue
func (comm *AmqpComm) Receive(queue string, p MessageProcessor) {
	ch, err := comm.conn.Channel()
	defer ch.Close()
	if err != nil {
		return
	}

	q, err := ch.QueueDeclare(
		queue, // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return
	}
	forever := make(chan bool)

	go func() {
		for d := range msgs {
			p.Process(d.Body)
		}
	}()

	log.Printf(" [*] Waiting for %s queue messages. To exit press CTRL+C", queue)
	<-forever
}

func (comm *AmqpComm) Send(queue string, msg []byte) error {
	ch, err := comm.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		queue, // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return err
	}

	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        msg,
		})

	if err != nil {
		return err
	}
	//	log.Printf(" [x] Sent %s", msg)
	return nil
}

func (comm *AmqpComm) Connect(uri string) error {
	connection, err := amqp.Dial(uri)
	if err != nil {
		return err
	}
	if connection != nil {
		comm.conn = connection
	}
	return nil
}

func (comm *AmqpComm) Close() error {
	return comm.conn.Close()
}
