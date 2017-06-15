package main

import (
	"log"
	"postTap/communicator"
)

var initNode *stap

func init() {
	initNode = &stap{scriptPath: "./stp_scripts/exec_init_node.stp", pid: 0, timeout: 0}
}

func main() {
	//	defer profile.Start(profile.CPUProfile).Stop()
	go WaitForCommand()

	initNode.Run()
	defer initNode.Stop()
}

func WaitForCommand() {
	commandQueue := new(communicator.AmqpComm)
	if err := commandQueue.Connect("amqp://guest:guest@localhost:5672"); err != nil {
		log.Fatalf("%s", err)
		return
	}
	defer commandQueue.Close()
	commandProcessor := new(Command)
	commandProcessor.RunningStp = map[int]*stap{}
	commandQueue.Receive("command", commandProcessor)
}
