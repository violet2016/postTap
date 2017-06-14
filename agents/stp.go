package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os/exec"
	"postTap/communicator"
	"strconv"
	"syscall"
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
	stp.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmdStdout, _ := stp.cmd.StdoutPipe()
	log.Printf("Monitoring stp %s running\n", stp.scriptPath)
	go readPipeandSend(cmdStdout)

	cmdErr, _ := stp.cmd.StderrPipe()

	go readPipe(cmdErr, "Error: ")
	stp.cmd.Start()
	if stp.timeout > 0 {
		select {
		case <-time.After(stp.timeout * time.Second):
			if err := stp.cmd.Process.Kill(); err != nil {
				log.Fatal("failed to kill: ", err)
			}
			log.Println("process killed as timeout reached")
			localComm := new(communicator.AmqpComm)
			if err := localComm.Connect("amqp://guest:guest@localhost:5672"); err != nil {
				log.Fatalf("%s", err)
			}
			defer localComm.Close()
			msg := []byte(fmt.Sprintf("%d|EndInstrument", stp.pid))
			if err := localComm.Send("probe", msg); err != nil {
				log.Fatalf("Cannot send EndInstrument")
			} else {
				log.Println("Send ", string(msg))
			}
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
	if stp.cmd.ProcessState == nil || stp.cmd.ProcessState.Exited() {
		return
	}
	pgid, err := syscall.Getpgid(stp.cmd.Process.Pid)
	if err == nil {
		syscall.Kill(-pgid, 15) // note the minus sign
	} else {
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
	localComm := new(communicator.AmqpComm)
	if err := localComm.Connect("amqp://guest:guest@localhost:5672"); err != nil {
		log.Fatalf("%s", err)
	}
	defer localComm.Close()
	r := bufio.NewReader(reader)
	var line []byte
	for true {
		line, _, _ = r.ReadLine()
		if line != nil {
			localComm.Send("probe", line)
		}
	}
}
