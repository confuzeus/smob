//go:build integration

package docker

// These tests drive the client beside the package, as the store equivalence
// test does: the daemon is real, and no fake server stands in for it. The CLI
// contract for an enabled source lives in test/integration/docker_test.go.

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const defaultSocket = "/var/run/docker.sock"

func TestConnectRealDaemon(t *testing.T) {
	socket := requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c, err := Connect(ctx, socket)
	if err != nil {
		t.Fatalf("connect to %s: %v", socket, err)
	}
	defer c.Close()

	if got := c.ClientVersion(); got != apiVersion {
		t.Fatalf("client API version = %q, want pinned %q; negotiation must stay disabled", got, apiVersion)
	}

	// Connect probes with /v1.41/info, not the versionless /_ping endpoint.
	// Repeating the versioned call proves the daemon accepts the pinned version
	// and that the client is still using it after the probe.
	info, err := c.Info(ctx)
	if err != nil {
		t.Fatalf("versioned /info call at API %s: %v", c.ClientVersion(), err)
	}
	if info.ServerVersion == "" {
		t.Fatal("daemon reported an empty server version")
	}
	t.Logf("connected to Docker %s at %s using API %s", info.ServerVersion, socket, c.ClientVersion())
}

func TestConnectUnreachableSocket(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		socket := filepath.Join(t.TempDir(), "missing.sock")
		requireConnectError(t, socket)
	})

	t.Run("closed", func(t *testing.T) {
		socket := filepath.Join(t.TempDir(), "closed.sock")
		listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
		if err != nil {
			t.Fatalf("listen on %s: %v", socket, err)
		}
		// Keep the socket file after closing so the probe exercises a refused
		// connection rather than a missing path.
		listener.SetUnlinkOnClose(false)
		if err := listener.Close(); err != nil {
			t.Fatalf("close listener on %s: %v", socket, err)
		}
		requireConnectError(t, socket)
	})
}

// requireDocker returns the configured Docker socket or skips when it is not
// present. Strict mode turns the skip into a failure.
func requireDocker(t *testing.T) string {
	t.Helper()
	socket := os.Getenv("SMOB_TEST_DOCKER_SOCKET")
	if socket == "" {
		socket = defaultSocket
	}
	if _, err := os.Stat(socket); err != nil {
		if os.Getenv("SMOB_TEST_STRICT") == "1" {
			t.Fatalf("SMOB_TEST_STRICT=1 but Docker socket %s is unavailable: %v", socket, err)
		}
		t.Skipf("Docker socket %s is unavailable (%v); set SMOB_TEST_DOCKER_SOCKET or run `just test-all`", socket, err)
	}
	return socket
}

// requireConnectError asserts that Connect fails promptly and names the socket.
func requireConnectError(t *testing.T, socket string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := Connect(ctx, socket)
	if c != nil {
		_ = c.Close()
		t.Fatalf("Connect(%s) returned a client, want an error", socket)
	}
	if err == nil {
		t.Fatalf("Connect(%s) succeeded, want an error", socket)
	}
	if !strings.Contains(err.Error(), socket) {
		t.Fatalf("error %q does not name the socket %q", err, socket)
	}
}
