package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os/exec"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// SendtoMQ send message to mq
func SendtoMQ(message []byte) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		})
	log.Printf(" [x] Sent %s", message)
	failOnError(err, "Failed to publish a message")
}

func main() {
	myCmd := exec.Command("stap", "../stp_scripts/query.stp")
	cmdOut, _ := myCmd.StdoutPipe()
	cmdErr, _ := myCmd.StderrPipe()

	go readPipeandSend(cmdOut)
	go readPipe(cmdErr, "Error: ")
	myCmd.Start()

	var input string
	fmt.Scanln(&input)
}

func readPipe(reader io.Reader, prefix string) {
	r := bufio.NewReader(reader)
	var outStr string
	var line []byte
	for true {
		line, _, _ = r.ReadLine()
		if line != nil {
			outStr = string(line)
			fmt.Println(prefix + outStr)
		}
	}
}

func readPipeandSend(reader io.Reader) {
	r := bufio.NewReader(reader)
	var line []byte
	for true {
		line, _, _ = r.ReadLine()
		if line != nil {
			SendtoMQ(line)
		}
	}
}
