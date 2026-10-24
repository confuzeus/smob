package docker

import (
	"cmp"
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

// Container identifies one running container discovered from the daemon. It
// carries the identity the metric layer will need for labels; it holds no
// samples or metric state.
type Container struct {
	// ID is the full container ID. A re-created container has a new ID.
	ID string
	// Name is the primary container name without Docker's leading slash.
	Name string
	// Image is the image reference reported by the container list.
	Image string
}

// discover returns the running containers, minus any whose name matches
// exclude. The daemon is queried with All disabled, so stopped containers are
// not discovered: they have no live stats and would only churn the series.
//
// exclude is unanchored, like the process source's comm filter, and is matched
// against the container name without the leading slash. IDs are not matched
// because a short pattern would accidentally match many hex IDs. Anchoring
// with ^ and $ selects an exact name.
func discover(ctx context.Context, c *client.Client, exclude *regexp.Regexp) ([]Container, error) {
	listCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	summaries, err := c.ContainerList(listCtx, container.ListOptions{All: false})
	if err != nil {
		return nil, fmt.Errorf("docker: list containers: %w", err)
	}

	containers := make([]Container, 0, len(summaries))
	for _, summary := range summaries {
		name := containerName(summary)
		if name != "" && exclude != nil && exclude.MatchString(name) {
			continue
		}
		if name == "" {
			// Docker did not report a name. Keep the ID as stable metadata,
			// but never apply the name filter to it.
			name = summary.ID
		}
		containers = append(containers, Container{
			ID:    summary.ID,
			Name:  name,
			Image: summary.Image,
		})
	}
	slices.SortFunc(containers, func(a, b Container) int {
		if n := cmp.Compare(a.Name, b.Name); n != 0 {
			return n
		}
		return cmp.Compare(a.ID, b.ID)
	})
	return containers, nil
}

// containerName returns the primary name without the leading slash, or an empty
// string when the daemon reported none.
func containerName(summary container.Summary) string {
	for _, name := range summary.Names {
		name = strings.TrimPrefix(name, "/")
		if name != "" {
			return name
		}
	}
	return ""
}
