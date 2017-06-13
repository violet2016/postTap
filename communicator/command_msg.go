package communicator

type CommandMsg struct {
	CommandName string
	Script      []byte
	Pid         int
}
