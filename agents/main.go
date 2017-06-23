package main

import (
	"bytes"
	"io/ioutil"
	"log"
	"postTap/common"
	"postTap/communicator"
)

var initNode *stap

func init() {
	bfile, err := ioutil.ReadFile("./stp_scripts/exec_plan.template")
	if err != nil {
		log.Fatal(err)
		return
	}
	replaceall := bytes.Replace(bfile, []byte("PLACEHOLDER_POSTGRES"), common.Which("postgres"), -1)
	err = ioutil.WriteFile("./stp_scripts/exec_plan.stp", replaceall, 0644)
	if err != nil {
		log.Fatal(err)
		return
	}
	initNode = &stap{scriptPath: "./stp_scripts/exec_plan.stp", pid: 0, timeout: 0}
}

func main() {
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
