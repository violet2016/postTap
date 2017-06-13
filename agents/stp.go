package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os/exec"
	"strconv"
	"time"
)

type stap struct {
	scriptPath string
	pid        int
	timeout    time.Duration
	cmd        *exec.Cmd
}

func (stp *stap) Run() {
	arg := []string{}
	if stp.pid != 0 {
		arg = append(arg, "-x", strconv.Itoa(stp.pid))
	}
	arg = append(arg, stp.scriptPath)
	stp.cmd = exec.Command("stap", arg...)
	cmdOut, _ := stp.cmd.StdoutPipe()
	cmdErr, _ := stp.cmd.StderrPipe()
	go readPipeandSend(cmdOut)
	go readPipe(cmdErr, "Error: ")
	stp.cmd.Start()
	if stp.timeout > 0 {
		select {
		case <-time.After(stp.timeout * time.Second):
			if err := stp.cmd.Process.Kill(); err != nil {
				log.Fatal("failed to kill: ", err)
			}
			log.Println("process killed as timeout reached")
		}
	} else {
		var input string
		fmt.Scanln(&input)
	}
}

func (stp *stap) Stop() {
	if stp.cmd == nil {
		return
	}
	if stp.cmd.ProcessState.Exited() {
		return
	}
	if err := stp.cmd.Process.Kill(); err != nil {
		log.Fatal("Fail to stop: ", err)
	}
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
