//go:build linux

package fusefs

import (
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestReceiveFuseFD(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "fuse.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	source, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()

	sendErr := make(chan error, 1)
	go func() {
		conn, err := listener.AcceptUnix()
		if err != nil {
			sendErr <- err
			return
		}
		defer conn.Close()
		file, err := conn.File()
		if err != nil {
			sendErr <- err
			return
		}
		defer file.Close()
		sendErr <- syscall.Sendmsg(int(file.Fd()), []byte{1}, syscall.UnixRights(int(source.Fd())), nil, 0)
	}()

	fd, err := receiveFuseFD(socketPath, DefaultFuseFDHandshakeTimeout)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fd)
	if err := <-sendErr; err != nil {
		t.Fatal(err)
	}

	var got, want syscall.Stat_t
	if err := syscall.Fstat(fd, &got); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Fstat(int(source.Fd()), &want); err != nil {
		t.Fatal(err)
	}
	if got.Dev != want.Dev || got.Ino != want.Ino {
		t.Fatalf("received descriptor identifies dev=%d ino=%d, want dev=%d ino=%d", got.Dev, got.Ino, want.Dev, want.Ino)
	}
}

func TestReceiveFuseFDRejectsMessageWithoutDescriptor(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "fuse.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	sendErr := make(chan error, 1)
	go func() {
		conn, err := listener.AcceptUnix()
		if err != nil {
			sendErr <- err
			return
		}
		defer conn.Close()
		_, err = conn.Write([]byte{1})
		sendErr <- err
	}()

	if _, err := receiveFuseFD(socketPath, DefaultFuseFDHandshakeTimeout); err == nil {
		t.Fatal("receiveFuseFD unexpectedly succeeded without a descriptor")
	}
	if err := <-sendErr; err != nil {
		t.Fatal(err)
	}
}

func TestExternalMountRejectsArtifactFSUnmount(t *testing.T) {
	mount := &mountedFSWrapper{externallyManaged: true}
	if err := mount.Unmount(); err != ErrExternalMountManaged {
		t.Fatalf("Unmount() error = %v, want %v", err, ErrExternalMountManaged)
	}
}
