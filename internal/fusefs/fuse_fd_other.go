//go:build !linux

package fusefs

import "fmt"

func receiveFuseFD(string) (int, error) {
	return -1, fmt.Errorf("FUSE descriptor sockets are supported only on Linux")
}
