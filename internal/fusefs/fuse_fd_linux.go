//go:build linux

package fusefs

import (
	"fmt"
	"net"
	"syscall"
	"time"
)

// receiveFuseFD receives exactly one file descriptor over socketPath. The
// descriptor is owned by the caller on success; fuse.Mount takes ownership
// when it is passed through its /dev/fd/N mount path.
func receiveFuseFD(socketPath string, timeout time.Duration) (int, error) {
	conn, err := net.DialTimeout("unix", socketPath, timeout)
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return -1, fmt.Errorf("expected Unix socket connection, got %T", conn)
	}
	if err := unixConn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return -1, fmt.Errorf("set descriptor receive deadline: %w", err)
	}

	data := make([]byte, 4096)
	oob := make([]byte, syscall.CmsgSpace(4))
	n, oobn, flags, _, err := unixConn.ReadMsgUnix(data, oob)
	if err != nil {
		return -1, err
	}
	if n == 0 {
		return -1, fmt.Errorf("received empty FUSE descriptor message")
	}
	if flags&syscall.MSG_CTRUNC != 0 {
		return -1, fmt.Errorf("received truncated FUSE descriptor control message")
	}

	messages, err := syscall.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return -1, fmt.Errorf("parse FUSE descriptor control message: %w", err)
	}
	var fds []int
	for _, message := range messages {
		received, err := syscall.ParseUnixRights(&message)
		if err == nil {
			fds = append(fds, received...)
		}
	}
	if len(fds) != 1 {
		for _, fd := range fds {
			_ = syscall.Close(fd)
		}
		return -1, fmt.Errorf("received %d FUSE descriptors, want exactly one", len(fds))
	}
	syscall.CloseOnExec(fds[0])

	return fds[0], nil
}
