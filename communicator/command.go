package communicator

import (
	"encoding/json"
	"fmt"
	"os"
)

type Command struct {
	CommandName string
	Script      []byte
	Pid         int
}

func (command *Command) WriteToFile() {
	var file *os.File
	var err error
	filepath := fmt.Sprintf("/tmp/%d.stp", command.Pid)
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		file, err = os.Create(filepath)
		if err != nil {
			return
		}

	} else {
		file, err = os.Open(filepath)
	}
	if err != nil {
		return
	}
	file.Write(command.Script)
}

func (command *Command) Process(msg []byte) error {
	err := json.Unmarshal(msg, command)
	if err == nil {
		if command.CommandName == "RUN" && len(command.Script) > 0 {
			command.WriteToFile()
		}
	}
	return err
}
