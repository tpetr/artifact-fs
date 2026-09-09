package daemon

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudflare/artifact-fs/internal/fusefs"
	"github.com/cloudflare/artifact-fs/internal/model"
)

type countedMount struct {
	done     chan struct{}
	once     sync.Once
	unmounts atomic.Int32
}

func newCountedMount() *countedMount { return &countedMount{done: make(chan struct{})} }

func (m *countedMount) Join(context.Context) error { <-m.done; return nil }
func (m *countedMount) Unmount() error {
	m.unmounts.Add(1)
	m.once.Do(func() { close(m.done) })
	return nil
}

func newDeferredTestService(t *testing.T) (*Service, string, *atomic.Int32, *countedMount) {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	svc, err := New(ctx, root, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	mountRoot := filepath.Join(root, "private-mnt")
	svc.SetMountRoot(mountRoot)
	var mounts atomic.Int32
	mounted := newCountedMount()
	svc.mountWithGate = func(model.RepoConfig, *fusefs.Resolver, *fusefs.Engine, *fusefs.ReadyGate) (fusefs.MountedFS, *fusefs.ArtifactFuse, error) {
		mounts.Add(1)
		return mounted, nil, nil
	}
	return svc, mountRoot, &mounts, mounted
}

func deferredRemote(t *testing.T) string {
	t.Helper()
	remote := filepath.Join(t.TempDir(), "remote")
	runCmd(t, "git", "init", "--initial-branch", "main", remote)
	if err := os.WriteFile(filepath.Join(remote, "README.md"), []byte("ready\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runCmd(t, "git", "-C", remote, "add", "README.md")
	runCmd(t, "git", "-C", remote, "-c", "user.name=test", "-c", "user.email=test@example.com", "commit", "-m", "init")
	return remote
}

func TestAwaitRepoMountsBeforeSyncRegistrationAndActivatesInPlace(t *testing.T) {
	ctx := context.Background()
	sidecar, mountRoot, mounts, mounted := newDeferredTestService(t)
	defer sidecar.Close()
	if err := sidecar.AwaitRepo(ctx, "repo"); err != nil {
		t.Fatal(err)
	}
	if got := mounts.Load(); got != 1 {
		t.Fatalf("mount calls before registration = %d, want 1", got)
	}

	remote := deferredRemote(t)
	if err := sidecar.AddRepo(ctx, model.RepoConfig{Name: "repo", RemoteURL: remote, Branch: "main", MountRoot: mountRoot, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := sidecar.syncRepos(ctx); err != nil {
		t.Fatal(err)
	}
	sidecar.mu.Lock()
	rt := sidecar.running[model.RepoID("repo")]
	sidecar.mu.Unlock()
	if rt == nil || rt.gate == nil || rt.deferred {
		t.Fatalf("runtime was not activated in place: %#v", rt)
	}
	if err := rt.gate.Wait(ctx); err != nil {
		t.Fatalf("gate = %v, want ready", err)
	}
	if got := mounts.Load(); got != 1 {
		t.Fatalf("mount calls after registration = %d, want unchanged", got)
	}
	if got := mounted.unmounts.Load(); got != 0 {
		t.Fatalf("unmount calls before shutdown = %d, want 0", got)
	}
}

func TestAwaitRepoAsyncTrustedPreparedGitDirActivatesInPlace(t *testing.T) {
	ctx := context.Background()
	sidecar, mountRoot, mounts, _ := newDeferredTestService(t)
	defer sidecar.Close()
	if err := sidecar.AwaitRepo(ctx, "repo"); err != nil {
		t.Fatal(err)
	}
	gitDir := createPreparedGitDir(t, t.TempDir())
	runCmd(t, "git", "--git-dir", gitDir, "fetch", "origin", "master:refs/remotes/origin/master")
	oid := strings.TrimSpace(runCmdOutput(t, "git", "--git-dir", gitDir, "rev-parse", "refs/remotes/origin/master"))
	runCmd(t, "git", "--git-dir", gitDir, "update-ref", "refs/heads/master", oid)
	if err := sidecar.AddRepoWithOptions(ctx, model.RepoConfig{
		Name: "repo", Branch: "refs/heads/master", MountRoot: mountRoot, GitDir: gitDir,
		PreparedGitDir: true, PreparedGitDirVerified: true, PreparedCommit: oid,
		RemoteRefreshDisabled: true, Enabled: true,
	}, AddRepoOptions{Async: true}); err != nil {
		t.Fatal(err)
	}
	if err := sidecar.syncRepos(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		cfg, err := sidecar.registry.GetRepo(ctx, "repo")
		return err == nil && cfg.PrepareState == model.PrepareStateReady
	})
	if err := sidecar.syncRepos(ctx); err != nil {
		t.Fatal(err)
	}
	if got := mounts.Load(); got != 1 {
		t.Fatalf("mount calls = %d, want one original mount", got)
	}
	sidecar.mu.Lock()
	rt := sidecar.running[model.RepoID("repo")]
	sidecar.mu.Unlock()
	if rt == nil || rt.deferred || rt.state.State != repoStateMounted {
		t.Fatalf("trusted prepared runtime = %#v", rt)
	}
}

func TestAwaitRepoFailureRetryConflictAndShutdown(t *testing.T) {
	ctx := context.Background()
	sidecar, mountRoot, mounts, mounted := newDeferredTestService(t)
	if err := sidecar.AwaitRepo(ctx, "repo"); err != nil {
		t.Fatal(err)
	}
	if err := sidecar.AddRepo(ctx, model.RepoConfig{Name: "repo", RemoteURL: "file:///does-not-exist", Branch: "main", MountRoot: mountRoot, Enabled: true}); err == nil {
		t.Fatal("expected initial preparation failure")
	}
	if err := sidecar.syncRepos(ctx); err != nil {
		t.Fatal(err)
	}
	sidecar.mu.Lock()
	gate := sidecar.running[model.RepoID("repo")].gate
	sidecar.mu.Unlock()
	if err := gate.Wait(ctx); err == nil {
		t.Fatal("failed preparation opened the gate")
	}
	if err := sidecar.AddRepo(ctx, model.RepoConfig{Name: "repo", RemoteURL: "file:///does-not-exist", Branch: "main", MountRoot: filepath.Join(mountRoot, "wrong"), Enabled: true}); err == nil || !strings.Contains(err.Error(), "awaiting") {
		t.Fatalf("conflicting registration error = %v, want awaiting mount error", err)
	}
	remote := deferredRemote(t)
	if err := sidecar.AddRepo(ctx, model.RepoConfig{Name: "repo", RemoteURL: remote, Branch: "main", MountRoot: mountRoot, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := sidecar.syncRepos(ctx); err != nil {
		t.Fatal(err)
	}
	if got := mounts.Load(); got != 1 {
		t.Fatalf("retry remounted: mount calls = %d", got)
	}
	if err := sidecar.Close(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if got := mounted.unmounts.Load(); got != 1 {
		t.Fatalf("shutdown unmount calls = %d, want 1", got)
	}
}

func TestAwaitRepoShutdownBeforeRegistration(t *testing.T) {
	ctx := context.Background()
	sidecar, _, mounts, mounted := newDeferredTestService(t)
	if err := sidecar.AwaitRepo(ctx, "repo"); err != nil {
		t.Fatal(err)
	}
	if got := mounts.Load(); got != 1 {
		t.Fatalf("mount calls = %d, want 1", got)
	}
	if err := sidecar.Close(); err != nil {
		t.Fatal(err)
	}
	if got := mounted.unmounts.Load(); got != 1 {
		t.Fatalf("shutdown while awaiting unmount calls = %d, want 1", got)
	}
}
