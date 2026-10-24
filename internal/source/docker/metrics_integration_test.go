//go:build integration

package docker

// These tests drive the metric computation beside the package. The daemon and
// container are real; no fake server stands in for either.

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

// TestContainerMetricsPlausible drives a CPU burner and an HTTP server in one
// container. It asserts plausibility, not exact live values: no first sample,
// sustained CPU above a floor, memory inside the limit, and non-zero network
// rates after traffic.
func TestContainerMetricsPlausible(t *testing.T) {
	c, ctx := connectDocker(t)
	id := startMetricsContainer(t, ctx, c)
	url := "http://" + net.JoinHostPort(containerIP(t, ctx, c, id), "8080") + "/blob"

	httpClient := &http.Client{Timeout: 2 * time.Second}
	waitForCondition(t, 30*time.Second, "container HTTP server", func() bool {
		resp, err := httpClient.Get(url)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	// Continuous traffic makes a non-zero rate likely in every interval, not
	// just the interval after a single burst.
	trafficCtx, stopTraffic := context.WithCancel(ctx)
	trafficDone := make(chan struct{})
	go func() {
		defer close(trafficDone)
		for {
			select {
			case <-trafficCtx.Done():
				return
			default:
			}
			resp, err := httpClient.Get(url)
			if err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}
	}()
	t.Cleanup(func() {
		stopTraffic()
		<-trafficDone
	})

	acq := newTestStatsAcquirer(c)
	streamCtx, stopStream := context.WithCancel(ctx)
	stream := acq.stream(streamCtx, []string{id})
	t.Cleanup(func() {
		stopStream()
		waitForStreamClose(t, stream, 15*time.Second)
	})

	acc := newAccumulator(newSupervisorTestLogger())
	deadline := time.After(45 * time.Second)

	var firstSeen, networkSeen, memorySeen bool
	cpuSamples := 0
	for !(cpuSamples >= 2 && networkSeen && memorySeen) {
		select {
		case payload, ok := <-stream:
			if !ok {
				t.Fatal("stats stream closed before the plausibility criteria were met")
			}
			obs := acc.observe(payload, time.Now())
			if !firstSeen {
				firstSeen = true
				if obs.cpuKnown {
					t.Fatal("first observation emitted a CPU rate")
				}
				if len(obs.networks) != 0 {
					t.Fatalf("first observation emitted %d network rates, want none", len(obs.networks))
				}
				if obs.memoryUsedBytes == 0 {
					t.Fatal("first observation emitted no memory gauge")
				}
			}

			if obs.cpuKnown {
				if math.IsNaN(obs.cpuPercent) || math.IsInf(obs.cpuPercent, 0) || obs.cpuPercent < 0 {
					t.Fatalf("CPU percent = %v, want a finite non-negative value", obs.cpuPercent)
				}
				if obs.cpuPercent > 10 {
					cpuSamples++
				}
			}

			if obs.memoryUsedBytes > 0 {
				memorySeen = true
			}
			limit := payload.stats.MemoryStats.Limit
			if limit > 0 && obs.memoryUsedBytes > limit {
				t.Fatalf("memory used %d exceeds limit %d", obs.memoryUsedBytes, limit)
			}

			for _, rate := range obs.networks {
				if math.IsNaN(rate.rx) || math.IsInf(rate.rx, 0) || rate.rx < 0 ||
					math.IsNaN(rate.tx) || math.IsInf(rate.tx, 0) || rate.tx < 0 {
					t.Fatalf("interface %s has an invalid rate: %+v", rate.name, rate)
				}
				if (rate.rxKnown && rate.rx > 0) || (rate.txKnown && rate.tx > 0) {
					networkSeen = true
				}
			}
		case <-deadline:
			t.Fatalf("timed out waiting for plausible metrics: cpu_samples=%d network=%v memory=%v",
				cpuSamples, networkSeen, memorySeen)
		}
	}
}

// startMetricsContainer starts a uniquely named busybox container that burns
// one CPU and serves a small file over HTTP on port 8080.
func startMetricsContainer(t *testing.T, ctx context.Context, c *client.Client) string {
	t.Helper()
	ensureImage(t, ctx, c, testImage)

	name := fmt.Sprintf("smob-metrics-%d-%d", os.Getpid(), time.Now().UnixNano())
	script := `mkdir -p /www
dd if=/dev/zero of=/www/blob bs=1024 count=64 2>/dev/null
httpd -f -p 8080 -h /www &
while :; do :; done`
	created, err := c.ContainerCreate(ctx,
		&container.Config{
			Image: testImage,
			Cmd:   []string{"sh", "-c", script},
		},
		&container.HostConfig{},
		nil,
		nil,
		name,
	)
	if err != nil {
		t.Fatalf("create container %s: %v", name, err)
	}
	t.Cleanup(func() {
		removeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := c.ContainerRemove(removeCtx, created.ID, container.RemoveOptions{Force: true}); err != nil {
			t.Logf("remove container %s: %v", created.ID, err)
		}
	})

	if err := c.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		t.Fatalf("start container %s: %v", created.ID, err)
	}
	return created.ID
}

// containerIP returns the container's first non-empty IPv4 address.
func containerIP(t *testing.T, ctx context.Context, c *client.Client, id string) string {
	t.Helper()
	var ip string
	waitForCondition(t, 20*time.Second, "container IP address", func() bool {
		info, err := c.ContainerInspect(ctx, id)
		if err != nil {
			return false
		}
		if info.NetworkSettings == nil {
			return false
		}
		for _, network := range info.NetworkSettings.Networks {
			if network != nil && network.IPAddress != "" {
				ip = network.IPAddress
				return true
			}
		}
		return false
	})
	return ip
}
