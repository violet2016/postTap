package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os/exec"
	"postTap/communicator"
	"strconv"
	"time"
)

var queryComm *communicator.AmqpComm

func init() {
	queryComm = new(communicator.AmqpComm)
}
func RunSTP(filepath string, pid int, timeout int) {
	arg := []string{}
	if pid != 0 {
		arg = append(arg, "-x", strconv.Itoa(pid))
	}
	arg = append(arg, filepath)
	myCmd := exec.Command("stap", arg...)
	cmdOut, _ := myCmd.StdoutPipe()
	cmdErr, _ := myCmd.StderrPipe()
	go readPipeandSend(cmdOut)
	go readPipe(cmdErr, "Error: ")
	myCmd.Start()
	if timeout > 0 {
		select {
		case <-time.After(time.Duration(timeout) * time.Second):
			if err := myCmd.Process.Kill(); err != nil {
				log.Fatal("failed to kill: ", err)
			}
			log.Println("process killed as timeout reached")
		}
	} else {
		var input string
		fmt.Scanln(&input)
	}
}
func main() {

	go WaitForCommand()
	RunSTP("./stp_scripts/exec_init_node.stp", 0, 0)
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
	if err := queryComm.Connect("amqp://guest:guest@localhost:5672"); err != nil {
		log.Fatalf("%s", err)
		return
	}
	defer queryComm.Close()
	r := bufio.NewReader(reader)
	var line []byte
	for true {
		line, _, _ = r.ReadLine()
		if line != nil {
			queryComm.Send("probe", line)
		}
	}
}

func WaitForCommand() {
	Command := new(communicator.AmqpComm)
	if err := Command.Connect("amqp://guest:guest@localhost:5672"); err != nil {
		log.Fatalf("%s", err)
		return
	}
	defer Command.Close()
	commandProcessor := new(communicator.Command)
	Command.Receive("command", commandProcessor)
}
