//go:build !windows

package fusefs

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

type recordingFS struct {
	fuseutil.NotImplementedFileSystem
	lookups atomic.Int32
}

func (fs *recordingFS) LookUpInode(context.Context, *fuseops.LookUpInodeOp) error {
	fs.lookups.Add(1)
	return nil
}

func TestGatedFileSystemBlocksUntilReady(t *testing.T) {
	next := &recordingFS{}
	gate := NewReadyGate(false)
	fs := NewGatedFileSystem(next, gate)

	done := make(chan error, 1)
	go func() {
		done <- fs.LookUpInode(context.Background(), &fuseops.LookUpInodeOp{})
	}()

	select {
	case err := <-done:
		t.Fatalf("LookUpInode returned before ready: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if got := next.lookups.Load(); got != 0 {
		t.Fatalf("lookups before ready = %d, want 0", got)
	}

	gate.MarkReady()
	if err := <-done; err != nil {
		t.Fatalf("LookUpInode after ready: %v", err)
	}
	if got := next.lookups.Load(); got != 1 {
		t.Fatalf("lookups after ready = %d, want 1", got)
	}
}

func TestGatedFileSystemFailedGateReturnsEIO(t *testing.T) {
	next := &recordingFS{}
	gate := NewReadyGate(false)
	gate.MarkFailed(errors.New("clone failed"))
	fs := NewGatedFileSystem(next, gate)

	err := fs.LookUpInode(context.Background(), &fuseops.LookUpInodeOp{})
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("LookUpInode error = %v, want EIO", err)
	}
	if got := next.lookups.Load(); got != 0 {
		t.Fatalf("lookups after failed gate = %d, want 0", got)
	}
}

func TestDeferredGatedFileSystemServesEmptyRootWhileAwaiting(t *testing.T) {
	gate := NewReadyGate(false)
	fs := NewDeferredGatedFileSystem(&recordingFS{}, gate)
	open := &fuseops.OpenDirOp{Inode: fuseops.RootInodeID}
	if err := fs.OpenDir(context.Background(), open); err != nil {
		t.Fatalf("OpenDir root = %v", err)
	}
	read := &fuseops.ReadDirOp{Handle: open.Handle, Dst: make([]byte, 128)}
	if err := fs.ReadDir(context.Background(), read); err != nil {
		t.Fatalf("ReadDir root = %v", err)
	}
	if read.BytesRead != 0 {
		t.Fatalf("empty deferred root bytes = %d, want 0", read.BytesRead)
	}
	if err := fs.ReleaseDirHandle(context.Background(), &fuseops.ReleaseDirHandleOp{Handle: open.Handle}); err != nil {
		t.Fatalf("ReleaseDirHandle root = %v", err)
	}
}
