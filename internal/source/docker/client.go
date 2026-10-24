// Package docker talks to the Docker Engine API over its Unix socket.
package docker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/docker/docker/client"
)

// apiVersion is the Docker Engine API version smob pins for every request. API
// 1.41 (Docker 20.10) is the first version with the one-shot container stats
// mode the source will use, and is old enough to support current hosts.
// Negotiation is deliberately disabled so request and response shapes do not
// change when the daemon is upgraded; an older daemon fails the probe instead
// of returning a response smob might misread.
const apiVersion = "1.41"

// probeTimeout bounds the startup probe so an unresponsive socket cannot block
// collection from starting.
const probeTimeout = 5 * time.Second

// Connect returns a Docker Engine client for the Unix socket at socket.
//
// socket is a filesystem path from sources.docker.socket. DOCKER_HOST and
// DOCKER_API_VERSION are deliberately ignored so the configuration file remains
// the single source of truth. The daemon is probed with a versioned /info
// request, so a missing, unreachable, or too-old daemon fails here rather than
// on the first collection tick. The caller owns the returned client and must
// close it.
func Connect(ctx context.Context, socket string) (*client.Client, error) {
	if socket == "" {
		return nil, errors.New("docker: socket path is empty")
	}

	c, err := client.NewClientWithOpts(
		client.WithHost("unix://"+socket),
		client.WithVersion(apiVersion),
	)
	if err != nil {
		return nil, fmt.Errorf("docker: create client for %s: %w", socket, err)
	}

	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	if _, err := c.Info(probeCtx); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("docker: connect to %s: %w", socket, err)
	}
	return c, nil
}
