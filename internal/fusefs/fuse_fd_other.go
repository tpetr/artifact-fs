//go:build !linux

package fusefs

import (
	"fmt"
	"time"
)

func receiveFuseFD(string, time.Duration) (int, error) {
	return -1, fmt.Errorf("FUSE descriptor sockets are supported only on Linux")
}
