package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type Command struct {
	CommandName string
	Script      []byte
	Pid         int
	RunningStp  map[int]*stap
}

func (command *Command) SaveScript(stp *stap) {
	var file *os.File
	var err error
	if _, err := os.Stat(stp.scriptPath); os.IsNotExist(err) {
		file, err = os.Create(stp.scriptPath)
		if err != nil {
			return
		}

	} else {
		file, err = os.Open(stp.scriptPath)
	}
	if err != nil {
		return
	}
	file.Write(command.Script)
}

func (command *Command) Process(msg []byte) error {
	err := json.Unmarshal(msg, command)
	if err == nil {
		switch command.CommandName {
		case "RUN":
			if len(command.Script) > 0 {
				stp := command.GetStap()
				command.SaveScript(stp)
				stp.Run()
			}
		case "STOP":
			stp := command.GetStap()
			stp.Stop()
			delete(command.RunningStp, command.Pid)
		}
	}
	return err
}
func (command *Command) GetStap() *stap {
	if stp, ok := command.RunningStp[command.Pid]; ok {
		return stp
	}
	stp := &stap{scriptPath: fmt.Sprintf("/tmp/%d.stp", command.Pid), pid: command.Pid, timeout: 5}
	command.RunningStp[command.Pid] = stp
	return stp
}
