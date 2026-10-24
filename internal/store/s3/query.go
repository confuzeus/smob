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

// Query yields matching samples in timestamp, metric, then canonical label
// order. It processes one UTC hour at a time and merges that hour's chunks, so
// memory is bounded by the compressed objects in one hour rather than the whole
// range.
//
// Listing and GETs are sequential. Wider prefix pruning and concurrent
// downloads with a bounded worker pool are a separate follow-up; this
// implementation only lists the hour prefixes and targets it needs.
func (s *Store) Query(ctx context.Context, q model.Query, yield func(model.Sample) error) error {
	filter, err := store.NewFilter(q)
	if err != nil {
		return err
	}
	groups, err := s.hourGroups(ctx, q)
	if err != nil {
		return err
	}

	emitted := 0
	for _, group := range groups {
		if err := ctx.Err(); err != nil {
			return err
		}

		limit := 0
		if q.Limit > 0 {
			limit = q.Limit - emitted
		}
		n, err := s.merge(ctx, group.refs, filter, limit, yield)
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

// hourGroups resolves the hour prefixes to scan. A bounded range is walked hour
// by hour; an unbounded range lists the configured root and groups keys by their
// chunk start, because the model treats a zero range boundary as unbounded.
func (s *Store) hourGroups(ctx context.Context, q model.Query) ([]hourGroup, error) {
	if q.Range.Start.IsZero() || q.Range.End.IsZero() {
		return s.walkHourGroups(ctx, q)
	}

	targets, err := s.targets(ctx, q)
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, nil
	}

	start := q.Range.Start.UTC().Truncate(time.Hour)
	last := q.Range.End.UTC().Add(-time.Nanosecond).Truncate(time.Hour)

	var groups []hourGroup
	for h := start; !h.After(last); h = h.Add(time.Hour) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		var remote, local []string
		for _, t := range targets {
			found, err := s.listHour(ctx, t.host, t.scope, h, q.Range)
			if err != nil {
				return nil, err
			}
			remote = append(remote, found...)

			found, err = s.spool.listHour(t.host, t.scope, h, q.Range)
			if err != nil {
				return nil, fmt.Errorf("list spool %s: %w", s.spool.dir, err)
			}
			local = append(local, found...)
		}
		refs := unionRefs(remote, local)
		if len(refs) == 0 {
			continue
		}
		groups = append(groups, hourGroup{refs: refs})
	}
	return groups, nil
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
	})
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
	})
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
	})
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

// objectStream decodes one downloaded chunk and holds its current sample.
type objectStream struct {
	key string
	dec *json.Decoder
	cur model.Sample
}

func openObjectStream(key string, data []byte) (*objectStream, error) {
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", key, err)
	}
	return &objectStream{key: key, dec: json.NewDecoder(gz)}, nil
}

func (s *objectStream) next() (model.Sample, bool, error) {
	var sample model.Sample
	err := s.dec.Decode(&sample)
	if errors.Is(err, io.EOF) {
		return model.Sample{}, false, nil
	}
	if err != nil {
		return model.Sample{}, false, fmt.Errorf("decode %s: %w", s.key, err)
	}
	if sample.Metric == "" {
		return model.Sample{}, false, fmt.Errorf("decode %s: sample has an empty metric", s.key)
	}
	return sample, true, nil
}

// objectHeap orders streams by their current sample.
type objectHeap struct {
	streams []*objectStream
}

func (h *objectHeap) Len() int { return len(h.streams) }
func (h *objectHeap) Less(i, j int) bool {
	return model.LessSample(h.streams[i].cur, h.streams[j].cur)
}
func (h *objectHeap) Swap(i, j int) {
	h.streams[i], h.streams[j] = h.streams[j], h.streams[i]
}
func (h *objectHeap) Push(x any) {
	h.streams = append(h.streams, x.(*objectStream))
}
func (h *objectHeap) Pop() any {
	old := h.streams
	n := len(old)
	x := old[n-1]
	h.streams = old[:n-1]
	return x
}

// readRef returns the bytes of one chunk. A nil slice with a nil error means
// the chunk disappeared between listing and opening, which is not a query
// failure.
func (s *Store) readRef(ctx context.Context, ref chunkRef) ([]byte, error) {
	if ref.local {
		data, err := s.spool.read(ref.key)
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read spool %s: %w", ref.key, err)
		}
		return data, nil
	}

	data, ok, err := s.getObject(ctx, s.objectKey(ref.key))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return data, nil
}

// merge reads refs from the bucket or spool and yields their matching samples
// as one sorted stream. A chunk that disappeared between listing and opening is
// skipped.
func (s *Store) merge(ctx context.Context, refs []chunkRef, filter *store.Filter, limit int, yield func(model.Sample) error) (int, error) {
	active := make([]*objectStream, 0, len(refs))
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		data, err := s.readRef(ctx, ref)
		if err != nil {
			return 0, err
		}
		if data == nil {
			continue
		}

		st, err := openObjectStream(ref.key, data)
		if err != nil {
			return 0, err
		}
		sample, ok, err := st.next()
		if err != nil {
			return 0, err
		}
		if !ok {
			continue
		}
		st.cur = sample
		active = append(active, st)
	}

	h := &objectHeap{streams: active}
	heap.Init(h)

	emitted, loops := 0, 0
	for h.Len() > 0 {
		loops++
		if loops%ctxCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return emitted, err
			}
		}

		st := h.streams[0]
		sample := st.cur
		if filter.Match(sample) {
			if err := yield(sample); err != nil {
				return emitted, err
			}
			emitted++
			if limit > 0 && emitted >= limit {
				return emitted, nil
			}
		}

		next, ok, err := st.next()
		if err != nil {
			return emitted, err
		}
		if !ok {
			heap.Pop(h)
			continue
		}
		st.cur = next
		heap.Fix(h, 0)
	}
	return emitted, nil
}

// getObject downloads one chunk, retrying transient failures. A missing key is
// reported as ok == false so the caller can skip it without failing the query.
func (s *Store) getObject(ctx context.Context, key string) ([]byte, bool, error) {
	var (
		data     []byte
		notFound bool
	)
	err := retry(ctx, func(ctx context.Context) error {
		data, notFound = nil, false

		out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			if isNotFound(err) {
				notFound = true
				return nil
			}
			return err
		}
		defer out.Body.Close()

		data, err = io.ReadAll(out.Body)
		if err != nil {
			return fmt.Errorf("read %s: %w", key, err)
		}
		return nil
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		return nil, false, fmt.Errorf("get %s: %w", key, err)
	}
	if notFound {
		return nil, false, nil
	}
	return data, true, nil
}
