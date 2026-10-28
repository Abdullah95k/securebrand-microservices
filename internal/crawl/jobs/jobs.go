// Package jobs keeps per-job task accounting in Redis so a job knows when
// every task it spawned has finished, regardless of retries, duplicates and
// redeliveries. Invariant: pending = |spawned| - |done|.
package jobs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// FinalizeSet holds job ids whose pending count reached zero.
const FinalizeSet = "jobs:finalize"

const ttl = 7 * 24 * time.Hour

// Post comment-collection states recorded per job.
const (
	PostPending   = "pending"
	PostComplete  = "complete"
	PostTruncated = "truncated"
	PostSkipped   = "skipped" // unchanged since the last scrape, or no comments
	PostFailed    = "failed"
)

func key(job, name string) string { return "{job:" + job + "}:" + name }

var spawnScript = redis.NewScript(`
local added = 0
for i = 3, #ARGV do
  if redis.call('SADD', KEYS[1], ARGV[i]) == 1 then
    added = added + 1
  end
end
if added > 0 then redis.call('INCRBY', KEYS[2], added) end
redis.call('PEXPIRE', KEYS[1], ARGV[1])
redis.call('PEXPIRE', KEYS[2], ARGV[1])
redis.call('SET', KEYS[3], ARGV[2], 'PX', ARGV[1])
return added
`)

var doneScript = redis.NewScript(`
if redis.call('SISMEMBER', KEYS[1], ARGV[3]) == 0 then
  return -1
end
local pending = tonumber(redis.call('GET', KEYS[3]) or '0')
if redis.call('SADD', KEYS[2], ARGV[3]) == 1 then
  pending = redis.call('DECR', KEYS[3])
  for i = 4, #ARGV, 2 do
    redis.call('HINCRBY', KEYS[4], ARGV[i], ARGV[i+1])
  end
end
redis.call('PEXPIRE', KEYS[2], ARGV[1])
redis.call('PEXPIRE', KEYS[3], ARGV[1])
redis.call('PEXPIRE', KEYS[4], ARGV[1])
redis.call('SET', KEYS[5], ARGV[2], 'PX', ARGV[1])
return pending
`)

// Tracker records spawns and completions.
type Tracker struct {
	r *redis.Client
}

func New(r *redis.Client) *Tracker { return &Tracker{r: r} }

// Spawn registers child task ids before they are produced.
func (t *Tracker) Spawn(ctx context.Context, job string, taskIDs ...string) error {
	if len(taskIDs) == 0 || job == "" {
		return nil
	}
	args := []any{ttl.Milliseconds(), time.Now().Unix()}
	for _, id := range taskIDs {
		args = append(args, id)
	}
	return spawnScript.Run(ctx, t.r, []string{key(job, "spawned"), key(job, "pending"), key(job, "last")}, args...).Err()
}

// Done marks a task finished and adds counters. When the job's pending count
// reaches zero the job is queued for finalisation.
func (t *Tracker) Done(ctx context.Context, job, taskID string, counters map[string]int64) (int64, error) {
	if job == "" {
		return 0, nil
	}
	args := []any{ttl.Milliseconds(), time.Now().Unix(), taskID}
	for k, v := range counters {
		args = append(args, k, v)
	}
	pending, err := doneScript.Run(ctx, t.r, []string{key(job, "spawned"), key(job, "done"), key(job, "pending"),
		key(job, "counters"), key(job, "last")}, args...).Int64()
	if err != nil {
		return 0, err
	}
	if pending == -1 {
		return 0, fmt.Errorf("jobs: task %s was never spawned for job %s", taskID, job)
	}
	if pending == 0 {
		if err := t.r.SAdd(ctx, FinalizeSet, job).Err(); err != nil {
			return 0, err
		}
	}
	return pending, nil
}

// SetPost records a post's comment-collection state for the job.
func (t *Tracker) SetPost(ctx context.Context, job string, postID int64, createdUnix int64, reported int64, state string) error {
	k := key(job, "posts")
	v := strconv.FormatInt(createdUnix, 10) + "|" + strconv.FormatInt(reported, 10) + "|" + state
	pipe := t.r.TxPipeline()
	pipe.HSet(ctx, k, strconv.FormatInt(postID, 10), v)
	pipe.PExpire(ctx, k, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// UpdatePostState changes only the state part of a recorded post.
func (t *Tracker) UpdatePostState(ctx context.Context, job string, postID int64, state string) error {
	k := key(job, "posts")
	f := strconv.FormatInt(postID, 10)
	cur, err := t.r.HGet(ctx, k, f).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	parts := strings.SplitN(cur, "|", 3)
	if len(parts) != 3 {
		parts = []string{"0", "0", ""}
	}
	parts[2] = state
	return t.r.HSet(ctx, k, f, strings.Join(parts, "|")).Err()
}

// PostState is one entry of Posts.
type PostState struct {
	PostID    int64
	CreatedAt time.Time
	Reported  int64
	State     string
}

// Posts returns every post recorded for the job.
func (t *Tracker) Posts(ctx context.Context, job string) ([]PostState, error) {
	m, err := t.r.HGetAll(ctx, key(job, "posts")).Result()
	if err != nil {
		return nil, err
	}
	out := make([]PostState, 0, len(m))
	for f, v := range m {
		id, _ := strconv.ParseInt(f, 10, 64)
		parts := strings.SplitN(v, "|", 3)
		if len(parts) != 3 {
			continue
		}
		c, _ := strconv.ParseInt(parts[0], 10, 64)
		r, _ := strconv.ParseInt(parts[1], 10, 64)
		out = append(out, PostState{PostID: id, CreatedAt: time.Unix(c, 0).UTC(), Reported: r, State: parts[2]})
	}
	return out, nil
}

// Status is the live state of a job.
type Status struct {
	Pending      int64
	Spawned      int64
	Done         int64
	Counters     map[string]int64
	LastActivity time.Time
}

func (t *Tracker) Status(ctx context.Context, job string) (Status, error) {
	pipe := t.r.Pipeline()
	pending := pipe.Get(ctx, key(job, "pending"))
	spawned := pipe.SCard(ctx, key(job, "spawned"))
	done := pipe.SCard(ctx, key(job, "done"))
	counters := pipe.HGetAll(ctx, key(job, "counters"))
	last := pipe.Get(ctx, key(job, "last"))
	_, err := pipe.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		return Status{}, err
	}
	s := Status{Spawned: spawned.Val(), Done: done.Val(), Counters: map[string]int64{}}
	s.Pending, _ = strconv.ParseInt(pending.Val(), 10, 64)
	for k, v := range counters.Val() {
		s.Counters[k], _ = strconv.ParseInt(v, 10, 64)
	}
	if l, err := strconv.ParseInt(last.Val(), 10, 64); err == nil {
		s.LastActivity = time.Unix(l, 0).UTC()
	}
	return s, nil
}

// Cancel flags a job; workers check it between pages.
func (t *Tracker) Cancel(ctx context.Context, job string) error {
	return t.r.Set(ctx, key(job, "cancel"), "1", ttl).Err()
}

func (t *Tracker) Cancelled(ctx context.Context, job string) bool {
	if job == "" {
		return false
	}
	n, err := t.r.Exists(ctx, key(job, "cancel")).Result()
	return err == nil && n > 0
}

// PopFinalize returns up to n jobs ready for finalisation.
func (t *Tracker) PopFinalize(ctx context.Context, n int64) ([]string, error) {
	return t.r.SPopN(ctx, FinalizeSet, n).Result()
}

// Requeue puts a job back in the finalize set (finaliser not ready yet).
func (t *Tracker) Requeue(ctx context.Context, job string) error {
	return t.r.SAdd(ctx, FinalizeSet, job).Err()
}

// Expire shortens the retention of a finished job's keys.
func (t *Tracker) Expire(ctx context.Context, job string, d time.Duration) error {
	pipe := t.r.Pipeline()
	for _, n := range []string{"spawned", "done", "pending", "counters", "last", "posts", "cancel"} {
		pipe.PExpire(ctx, key(job, n), d)
	}
	_, err := pipe.Exec(ctx)
	return err
}
