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
	quit       chan bool
	status     int
}

func (stp *stap) Run() {
	arg := []string{"-w"}

	if stp.pid != 0 {
		arg = append(arg, "-x", strconv.Itoa(stp.pid))
	}
	arg = append(arg, stp.scriptPath)
	stp.cmd = exec.Command("stap", arg...)
	stp.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmdStdout, _ := stp.cmd.StdoutPipe()
	log.Printf("Monitoring stp %s running\n", stp.scriptPath)

	stp.quit = make(chan bool)
	go readPipeandSend(cmdStdout, stp.quit)

	cmdErr, _ := stp.cmd.StderrPipe()

	go readPipe(cmdErr, "Error: ", stp.quit)
	stp.cmd.Start()
	stp.status = 1
	if stp.timeout > 0 {
		select {
		case <-time.After(stp.timeout * time.Second):
			stp.Stop()
			localComm := new(communicator.AmqpComm)
			if err := localComm.Connect("amqp://guest:guest@localhost:5672"); err != nil {
				log.Fatalf("%s", err)
			}
			defer localComm.Close()
			msg := []byte(fmt.Sprintf("%d|EndInstrument", stp.pid))
			if err := localComm.Send("probe", msg); err != nil {
				log.Fatalf("Cannot send EndInstrument")
			}
		}
	} else {
		var input string
		fmt.Scanln(&input)
	}
}

func (stp *stap) Stop() {
	log.Println("Stop process")
	if stp.cmd == nil || stp.cmd.Process == nil {
		log.Println("cmd does not exist")
		return
	}
	if stp.status == 1 {
		close(stp.quit)
	}

	defer stp.cmd.Wait()
	pgid, err := syscall.Getpgid(stp.cmd.Process.Pid)
	if err == nil {
		syscall.Kill(-pgid, 15) // note the minus sign
		log.Println("terminate process")
		stp.cmd = nil
		stp.status = 0
	} else {
		log.Fatal("Failed to call kill process:", err)
	}

}
func readPipe(reader io.Reader, prefix string, quit <-chan bool) {
	r := bufio.NewReader(reader)
	var outStr string
	var line []byte
	for {
		select {
		case <-quit:
			return
		default:
			line, _, _ = r.ReadLine()
			if line != nil {
				outStr = string(line)
				fmt.Println(prefix + outStr)
			}
		}

	}
}

func readPipeandSend(reader io.Reader, quit <-chan bool) {
	localComm := new(communicator.AmqpComm)
	if err := localComm.Connect("amqp://guest:guest@localhost:5672"); err != nil {
		log.Fatalf("%s", err)
	}
	defer localComm.Close()
	r := bufio.NewReader(reader)
	var line []byte
	for {
		select {
		case <-quit:
			return
		default:
			line, _, _ = r.ReadLine()
			if line != nil {
				localComm.Send("probe", line)
			}
		}
	}
}
