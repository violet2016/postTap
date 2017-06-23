package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

type Command struct {
	CommandName string
	Script      []byte
	Pid         int
	RunningStp  map[int]*stap
}

func (command *Command) SaveScript(stp *stap) {
	if _, err := os.Stat(stp.scriptPath); os.IsNotExist(err) {
		_, err = os.Create(stp.scriptPath)
		if err != nil {
			log.Printf("Error occurred during script opening: %s", err)
			return
		}
	}

	err := ioutil.WriteFile(stp.scriptPath, command.Script, 0644)
	if err != nil {
		log.Printf("Error occurred during script saving: %s", err)
	}
}

func (command *Command) Process(msg []byte) error {
	err := json.Unmarshal(msg, command)
	if err == nil {
		switch command.CommandName {
		case "RUN":
			stp := command.GetStap()
			if len(command.Script) > 0 {
				command.SaveScript(stp)
			}
			stp.Run()
			/*	case "STOP":
				stp := command.GetStap()
				stp.Stop()
				delete(command.RunningStp, command.Pid)*/
		}
	}
	return err
}
func (command *Command) GetStap() *stap {
	if stp, ok := command.RunningStp[command.Pid]; ok {
		return stp
	}
	stp := &stap{scriptPath: fmt.Sprintf("/tmp/%d.stp", command.Pid), pid: command.Pid, timeout: 10}
	command.RunningStp[command.Pid] = stp
	return stp
}
