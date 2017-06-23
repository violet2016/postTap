package common

import (
	"fmt"
	"os/exec"
)

// Which return the binary's path
func Which(name string) []byte {
	out, err := exec.Command("sh", "-c", fmt.Sprintf("which %s", name)).Output()
	if err != nil {
		fmt.Print(err)
	}
	if len(out) > 1 {
		return out[:len(out)-1]
	}
	return []byte{}
}
