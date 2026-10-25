package s3

import (
	"bytes"
	"compress/gzip"
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"path"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/confuzeus/smob/internal/model"
	"github.com/confuzeus/smob/internal/store"
	"github.com/confuzeus/smob/internal/store/chunk"
)

// ctxCheckInterval bounds how long a large merge can run without observing
// cancellation.
const ctxCheckInterval = 1024

// queryGetWorkers bounds concurrent bucket GETs within one window group. Query
// merges one window group at a time, so this is also the in-flight limit for the
// whole query. Eight lets a window shared by several hosts and scopes finish in
// one round over a slow link without bursting a small endpoint.
const queryGetWorkers = 8

// Query yields matching samples in timestamp, metric, then canonical label
// order. It processes one UTC hour at a time and, within that hour, one chunk
// window at a time: it lists the hour's prefixes, groups the chunks by window
// start, downloads and decodes the refs that share a start with a bounded
// worker pool, merges them, and yields the group before moving on. Chunk
// windows are disjoint, so samples in an earlier window precede samples in a
// later one; the output stays globally ordered while memory holds only one
// window group's decoded samples. Limit stops before any later group or hour is
// listed or fetched.
//
// Both range boundaries are required. The CLI always supplies them; a query
// unbounded at both ends would have to list the whole bucket and is rejected.
// A zero start or end alone is still accepted: that fallback discovers the full
// key set once and groups it by hour, because a host and scope path has to be
// known before an hour prefix can exist.
func (s *Store) Query(ctx context.Context, q model.Query, yield func(model.Sample) error) error {
	filter, err := store.NewFilter(q)
	if err != nil {
		return err
	}
	if q.Range.Start.IsZero() && q.Range.End.IsZero() {
		return errors.New("query: range start and end are both zero; set a start or an end so the query does not list the whole bucket")
	}

	if q.Range.Start.IsZero() || q.Range.End.IsZero() {
		groups, err := s.walkHourGroups(ctx, q)
		if err != nil {
			return err
		}
		return s.mergeGroups(ctx, q, filter, groups, yield)
	}

	targets, err := s.targets(ctx, q)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		return nil
	}

	emitted := 0
	start := q.Range.Start.UTC().Truncate(time.Hour)
	last := q.Range.End.UTC().Add(-time.Nanosecond).Truncate(time.Hour)
	for h := start; !h.After(last); h = h.Add(time.Hour) {
		if err := ctx.Err(); err != nil {
			return err
		}

		refs, err := s.hourRefs(ctx, targets, h, q.Range)
		if err != nil {
			return err
		}
		if len(refs) == 0 {
			continue
		}

		n, err := s.mergeHour(ctx, refs, filter, remainingLimit(q.Limit, emitted), yield)
		emitted += n
		if err != nil {
			return err
		}
		if q.Limit > 0 && emitted >= q.Limit {
			return nil
		}
	}
	return nil
}

// mergeGroups merges pre-grouped hours in order. It serves the unbounded range
// fallback, where the whole key set must be discovered before the first hour
// can be merged.
func (s *Store) mergeGroups(ctx context.Context, q model.Query, filter *store.Filter, groups []hourGroup, yield func(model.Sample) error) error {
	emitted := 0
	for _, group := range groups {
		if err := ctx.Err(); err != nil {
			return err
		}

		n, err := s.mergeHour(ctx, group.refs, filter, remainingLimit(q.Limit, emitted), yield)
		emitted += n
		if err != nil {
			return err
		}
		if q.Limit > 0 && emitted >= q.Limit {
			return nil
		}
	}
	return nil
}

// remainingLimit returns how many samples the next group may still emit, or 0
// when the query is unlimited.
func remainingLimit(limit, emitted int) int {
	if limit <= 0 {
		return 0
	}
	return limit - emitted
}

// hourGroup is the set of chunks that may contain samples for one UTC hour.
// Groups are kept in hour order, which keeps output globally sorted.
type hourGroup struct {
	refs []chunkRef
}

// chunkRef identifies one chunk by its relative key and where to read it. A
// remote ref is a bucket object; a local ref is a spool file that has not been
// uploaded yet.
type chunkRef struct {
	key   string
	local bool
}

// groupWindows orders refs by window start. Each group holds the refs that
// share one start, at most one chunk per host and scope plus collision
// suffixes, because only they can interleave in the merge.
func groupWindows(refs []chunkRef) ([][]chunkRef, error) {
	byStart := make(map[time.Time][]chunkRef, len(refs))
	for _, ref := range refs {
		start, _, err := chunk.ParseName(path.Base(ref.key))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", ref.key, err)
		}
		start = start.UTC()
		byStart[start] = append(byStart[start], ref)
	}

	starts := make([]time.Time, 0, len(byStart))
	for start := range byStart {
		starts = append(starts, start)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })

	groups := make([][]chunkRef, 0, len(starts))
	for _, start := range starts {
		groups = append(groups, byStart[start])
	}
	return groups, nil
}

// unionRefs overlays local spool keys on bucket keys. A key present in both is
// read from the bucket only, so an object whose spool deletion failed is not
// emitted twice. The result is sorted by key for deterministic reads.
func unionRefs(remote, local []string) []chunkRef {
	seen := make(map[string]struct{}, len(remote)+len(local))
	refs := make([]chunkRef, 0, len(remote)+len(local))
	for _, key := range remote {
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		refs = append(refs, chunkRef{key: key})
	}
	for _, key := range local {
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		refs = append(refs, chunkRef{key: key, local: true})
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].key < refs[j].key })
	return refs
}

// unionStrings returns the sorted union of two string slices.
func unionStrings(a, b []string) []string {
	set := make(map[string]struct{}, len(a)+len(b))
	for _, value := range a {
		set[value] = struct{}{}
	}
	for _, value := range b {
		set[value] = struct{}{}
	}
	return slices.Sorted(maps.Keys(set))
}

// target is one host/scope path to scan.
type target struct {
	host  string
	scope string
}

// hourRefs lists the bucket and spool chunks that may contain samples for one
// UTC hour. It lists only that hour's prefixes for the resolved targets, so no
// listing reaches outside the hour being merged. The result is sorted by key.
func (s *Store) hourRefs(ctx context.Context, targets []target, hour time.Time, r model.TimeRange) ([]chunkRef, error) {
	var remote, local []string
	for _, t := range targets {
		found, err := s.listHour(ctx, t.host, t.scope, hour, r)
		if err != nil {
			return nil, err
		}
		remote = append(remote, found...)

		found, err = s.spool.listHour(t.host, t.scope, hour, r)
		if err != nil {
			return nil, fmt.Errorf("list spool %s: %w", s.spool.dir, err)
		}
		local = append(local, found...)
	}
	return unionRefs(remote, local), nil
}

// walkHourGroups discovers chunks at any depth and groups them by UTC hour. It
// is the fallback for an unbounded start or end.
func (s *Store) walkHourGroups(ctx context.Context, q model.Query) ([]hourGroup, error) {
	prefix := s.rootPrefix()
	if q.Host != "" {
		if err := chunk.ValidateComponent("host", q.Host); err != nil {
			return nil, err
		}
		prefix += q.Host + "/"
		if q.Scope != "" {
			if err := chunk.ValidateComponent("scope", q.Scope); err != nil {
				return nil, err
			}
			prefix += q.Scope + "/"
		}
	}

	byHour := make(map[time.Time][]string)
	paginator := awss3.NewListObjectsV2Paginator(s.client, &awss3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	}, stopOnDuplicateToken)
	for paginator.HasMorePages() {
		page, err := nextListPage(ctx, paginator)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("list s3://%s/%s: %w", s.bucket, prefix, err)
		}
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			rel, ok := s.relativeKey(key)
			if !ok {
				continue
			}
			name := path.Base(rel)
			if !strings.HasSuffix(name, chunk.FileSuffix) {
				continue
			}
			start, end, err := chunk.ParseName(name)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", key, err)
			}
			if !chunk.Overlaps(start, end, q.Range) {
				continue
			}
			hour := start.UTC().Truncate(time.Hour)
			byHour[hour] = append(byHour[hour], rel)
		}
	}

	spooled, err := s.spool.list(ctx, q.Range, q.Host, q.Scope)
	if err != nil {
		return nil, fmt.Errorf("list spool %s: %w", s.spool.dir, err)
	}

	hours := make([]time.Time, 0, len(byHour)+len(spooled))
	seenHours := make(map[time.Time]struct{}, len(byHour)+len(spooled))
	for hour := range byHour {
		seenHours[hour] = struct{}{}
		hours = append(hours, hour)
	}
	for hour := range spooled {
		if _, ok := seenHours[hour]; ok {
			continue
		}
		hours = append(hours, hour)
	}
	sort.Slice(hours, func(i, j int) bool { return hours[i].Before(hours[j]) })

	groups := make([]hourGroup, 0, len(hours))
	for _, hour := range hours {
		refs := unionRefs(byHour[hour], spooled[hour])
		if len(refs) == 0 {
			continue
		}
		groups = append(groups, hourGroup{refs: refs})
	}
	return groups, nil
}

// relativeKey strips the configured root prefix from a bucket key. A key that
// is outside the prefix cannot come from this store and is ignored.
func (s *Store) relativeKey(key string) (string, bool) {
	prefix := s.rootPrefix()
	if prefix == "" {
		return key, true
	}
	if !strings.HasPrefix(key, prefix) {
		return "", false
	}
	return strings.TrimPrefix(key, prefix), true
}

// targets resolves the host/scope paths to scan. An explicit host or scope is
// trusted after validation; otherwise the directories are discovered with a
// delimiter listing.
func (s *Store) targets(ctx context.Context, q model.Query) ([]target, error) {
	hosts, err := s.hosts(ctx, q.Host)
	if err != nil {
		return nil, err
	}

	var targets []target
	for _, host := range hosts {
		scopes, err := s.scopes(ctx, host, q.Scope)
		if err != nil {
			return nil, err
		}
		for _, scope := range scopes {
			targets = append(targets, target{host: host, scope: scope})
		}
	}
	return targets, nil
}

func (s *Store) hosts(ctx context.Context, host string) ([]string, error) {
	if host != "" {
		if err := chunk.ValidateComponent("host", host); err != nil {
			return nil, err
		}
		return []string{host}, nil
	}
	remote, err := s.listComponents(ctx, s.rootPrefix())
	if err != nil {
		return nil, err
	}
	local, err := s.spool.hosts()
	if err != nil {
		return nil, fmt.Errorf("list spool %s: %w", s.spool.dir, err)
	}
	return unionStrings(remote, local), nil
}

func (s *Store) scopes(ctx context.Context, host, scope string) ([]string, error) {
	if scope != "" {
		if err := chunk.ValidateComponent("scope", scope); err != nil {
			return nil, err
		}
		return []string{scope}, nil
	}
	remote, err := s.listComponents(ctx, s.rootPrefix()+host+"/")
	if err != nil {
		return nil, err
	}
	local, err := s.spool.scopes(host)
	if err != nil {
		return nil, fmt.Errorf("list spool %s: %w", s.spool.dir, err)
	}
	return unionStrings(remote, local), nil
}

// listComponents returns the immediate child prefixes under prefix that can be
// a host or scope path component.
func (s *Store) listComponents(ctx context.Context, prefix string) ([]string, error) {
	var names []string
	paginator := awss3.NewListObjectsV2Paginator(s.client, &awss3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	}, stopOnDuplicateToken)
	for paginator.HasMorePages() {
		page, err := nextListPage(ctx, paginator)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("list s3://%s/%s: %w", s.bucket, prefix, err)
		}
		for _, common := range page.CommonPrefixes {
			name := strings.TrimSuffix(strings.TrimPrefix(aws.ToString(common.Prefix), prefix), "/")
			if chunk.IsComponent(name) {
				names = append(names, name)
			}
		}
	}
	sort.Strings(names)
	return names, nil
}

// listHour returns the relative keys in one host/scope/hour whose windows
// overlap r. A missing prefix is an empty hour, not an error.
func (s *Store) listHour(ctx context.Context, host, scope string, hour time.Time, r model.TimeRange) ([]string, error) {
	prefix := s.hourPrefix(host, scope, hour)

	var keys []string
	paginator := awss3.NewListObjectsV2Paginator(s.client, &awss3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	}, stopOnDuplicateToken)
	for paginator.HasMorePages() {
		page, err := nextListPage(ctx, paginator)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("list s3://%s/%s: %w", s.bucket, prefix, err)
		}
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			rel, ok := s.relativeKey(key)
			if !ok {
				continue
			}
			name := path.Base(rel)
			if !strings.HasSuffix(name, chunk.FileSuffix) {
				continue
			}
			start, end, err := chunk.ParseName(name)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", key, err)
			}
			if !chunk.Overlaps(start, end, r) {
				continue
			}
			keys = append(keys, rel)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

// stopOnDuplicateToken stops a listing when a backend repeats its continuation
// token, so a broken page sequence cannot loop forever.
func stopOnDuplicateToken(o *awss3.ListObjectsV2PaginatorOptions) {
	o.StopOnDuplicateToken = true
}

// nextListPage fetches one listing page, retrying transient failures. The
// paginator advances its continuation token only after a successful page, so a
// retry cannot skip or duplicate results.
func nextListPage(ctx context.Context, paginator *awss3.ListObjectsV2Paginator) (*awss3.ListObjectsV2Output, error) {
	var page *awss3.ListObjectsV2Output
	err := retry(ctx, func(ctx context.Context) error {
		var err error
		page, err = paginator.NextPage(ctx)
		return err
	})
	return page, err
}

// hourPrefix returns the object prefix for one host/scope/hour, with a trailing
// slash so it cannot match a neighbouring hour.
func (s *Store) hourPrefix(host, scope string, hour time.Time) string {
	key := path.Join(
		host,
		scope,
		hour.Format("2006"),
		hour.Format("01"),
		hour.Format("02"),
		hour.Format("15"),
	) + "/"
	return s.objectKey(key)
}

// sampleCursor walks one decoded chunk. The chunk is sorted by timestamp,
// metric, then canonical label order, so a heap over the cursors merges chunks
// in the same order.
type sampleCursor struct {
	samples []model.Sample
	pos     int
}

// sampleHeap orders cursors by their current sample.
type sampleHeap struct {
	cursors []*sampleCursor
}

func (h *sampleHeap) Len() int { return len(h.cursors) }
func (h *sampleHeap) Less(i, j int) bool {
	return model.LessSample(
		h.cursors[i].samples[h.cursors[i].pos],
		h.cursors[j].samples[h.cursors[j].pos],
	)
}
func (h *sampleHeap) Swap(i, j int) {
	h.cursors[i], h.cursors[j] = h.cursors[j], h.cursors[i]
}
func (h *sampleHeap) Push(x any) {
	h.cursors = append(h.cursors, x.(*sampleCursor))
}
func (h *sampleHeap) Pop() any {
	old := h.cursors
	n := len(old)
	x := old[n-1]
	h.cursors = old[:n-1]
	return x
}

// decodeSamples decodes one gzip JSONL chunk from r. It does not close r, and
// it retains only the decoded samples, never the compressed body.
func decodeSamples(key string, r io.Reader) ([]model.Sample, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", key, err)
	}
	defer gz.Close()

	var samples []model.Sample
	dec := json.NewDecoder(gz)
	for {
		var sample model.Sample
		err := dec.Decode(&sample)
		if errors.Is(err, io.EOF) {
			return samples, nil
		}
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", key, err)
		}
		if sample.Metric == "" {
			return nil, fmt.Errorf("decode %s: sample has an empty metric", key)
		}
		samples = append(samples, sample)
	}
}

// readRefSamples fetches one chunk and decodes it immediately. A nil slice with
// a nil error means the chunk disappeared between listing and opening, which is
// not a query failure.
func (s *Store) readRefSamples(ctx context.Context, ref chunkRef) ([]model.Sample, error) {
	if ref.local {
		return s.readSpoolSamples(ref.key)
	}

	samples, ok, err := s.getSamples(ctx, s.objectKey(ref.key))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return samples, nil
}

// readSpoolSamples decodes one spooled chunk. A missing file is skipped; the
// bytes are released as soon as the samples are decoded.
func (s *Store) readSpoolSamples(key string) ([]model.Sample, error) {
	data, err := s.spool.read(key)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read spool %s: %w", key, err)
	}
	return decodeSamples(key, bytes.NewReader(data))
}

// readWindow fetches and decodes every ref in one window group. Bucket chunks
// are downloaded and decoded concurrently; a worker keeps only the decoded
// samples, so the compressed body is released as soon as it has been read.
// Spool chunks are read on the calling goroutine. A nil entry means the chunk
// disappeared between listing and reading, which the merge skips.
//
// Downloads may finish out of order, but each result is stored at its ref
// position, so the caller merges a length-stable, key-ordered slice. At most
// queryGetWorkers GETs are in flight. The first error cancels the siblings and
// is returned once every worker has been joined. A cancellation that leaves
// refs unfetched is reported as an error, never mistaken for absence.
func (s *Store) readWindow(ctx context.Context, refs []chunkRef) ([][]model.Sample, error) {
	decoded := make([][]model.Sample, len(refs))
	remote := make([]int, 0, len(refs))
	for i, ref := range refs {
		if !ref.local {
			remote = append(remote, i)
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		samples, err := s.readRefSamples(ctx, ref)
		if err != nil {
			return nil, err
		}
		decoded[i] = samples
	}
	if len(remote) == 0 {
		return decoded, nil
	}

	// Sibling fetches are cancelled on the first error. On the success path, the
	// deferred cancel releases the child context before returning.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int)
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	workers := min(queryGetWorkers, len(remote))
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				samples, err := s.readRefSamples(ctx, refs[idx])
				if err != nil {
					errOnce.Do(func() {
						firstErr = err
						cancel()
					})
					return
				}
				decoded[idx] = samples
			}
		}()
	}

feed:
	for _, idx := range remote {
		select {
		case jobs <- idx:
		case <-ctx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	// The producer can stop on cancellation before dispatching every ref, and a
	// worker can exit without recording an error. Without this check, the
	// unfetched refs would look like missing chunks and be skipped silently.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return decoded, nil
}

// mergeHour merges one hour's refs one window group at a time, stopping after
// limit matching samples when limit is positive. Groups run in window-start
// order, so an earlier group is fully yielded before a later one is fetched.
func (s *Store) mergeHour(ctx context.Context, refs []chunkRef, filter *store.Filter, limit int, yield func(model.Sample) error) (int, error) {
	groups, err := groupWindows(refs)
	if err != nil {
		return 0, err
	}

	emitted := 0
	for _, group := range groups {
		if err := ctx.Err(); err != nil {
			return emitted, err
		}

		n, err := s.mergeWindow(ctx, group, filter, remainingLimit(limit, emitted), yield)
		emitted += n
		if err != nil {
			return emitted, err
		}
		if limit > 0 && emitted >= limit {
			return emitted, nil
		}
	}
	return emitted, nil
}

// mergeWindow downloads and decodes one window group's refs and yields their
// matching samples as one sorted stream. A chunk that disappeared between
// listing and opening is skipped. Reaching limit stops the merge before any
// later group is fetched.
func (s *Store) mergeWindow(ctx context.Context, refs []chunkRef, filter *store.Filter, limit int, yield func(model.Sample) error) (int, error) {
	decoded, err := s.readWindow(ctx, refs)
	if err != nil {
		return 0, err
	}

	active := make([]*sampleCursor, 0, len(decoded))
	for _, samples := range decoded {
		if len(samples) == 0 {
			continue
		}
		active = append(active, &sampleCursor{samples: samples})
	}

	h := &sampleHeap{cursors: active}
	heap.Init(h)

	emitted, loops := 0, 0
	for h.Len() > 0 {
		loops++
		if loops%ctxCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return emitted, err
			}
		}

		cursor := h.cursors[0]
		sample := cursor.samples[cursor.pos]
		if filter.Match(sample) {
			if err := yield(sample); err != nil {
				return emitted, err
			}
			emitted++
			if limit > 0 && emitted >= limit {
				return emitted, nil
			}
		}

		cursor.pos++
		if cursor.pos >= len(cursor.samples) {
			heap.Pop(h)
			continue
		}
		heap.Fix(h, 0)
	}
	return emitted, nil
}

// getBody downloads one object and passes its body to consume before closing
// it. A missing key is reported as found == false. Transient failures,
// including a body that fails mid-decode, are retried with a fresh GET.
func (s *Store) getBody(ctx context.Context, key string, consume func(io.Reader) error) (found bool, err error) {
	err = retry(ctx, func(ctx context.Context) error {
		found = false

		out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			if isNotFound(err) {
				return nil
			}
			return err
		}
		defer out.Body.Close()

		found = true
		return consume(out.Body)
	})
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return false, fmt.Errorf("get %s: %w", key, err)
	}
	return found, nil
}

// getObject returns one object's bytes, retrying transient failures. A missing
// key is reported as ok == false so the caller can skip it without failing the
// query.
func (s *Store) getObject(ctx context.Context, key string) ([]byte, bool, error) {
	var data []byte
	found, err := s.getBody(ctx, key, func(r io.Reader) error {
		var err error
		data, err = io.ReadAll(r)
		if err != nil {
			return fmt.Errorf("read %s: %w", key, err)
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	return data, true, nil
}

// getSamples streams one chunk through gzip and JSON decoding without holding
// the compressed body. A missing key is reported as ok == false.
func (s *Store) getSamples(ctx context.Context, key string) ([]model.Sample, bool, error) {
	var samples []model.Sample
	found, err := s.getBody(ctx, key, func(r io.Reader) error {
		var err error
		samples, err = decodeSamples(key, r)
		return err
	})
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	return samples, true, nil
}
