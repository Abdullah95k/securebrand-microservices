package discovery

import (
	"context"
	"errors"
	"time"
)

// Result is one post a search found.
type Result struct {
	// PostID is the post's Facebook id (as text: story ids are not always
	// numeric).
	PostID   string
	PostTime time.Time
	// OwnerID and OwnerRef name who posted: the numeric id and/or the
	// vanity or URL (either may be empty, not both).
	OwnerID  int64
	OwnerRef string
	// OwnerKind is "page", "profile" or "group"; groups are skipped.
	OwnerKind string
	// Text is the post's text as the search returned it, "" if none; when
	// set it must contain the keyword (Mentions) for the result to count.
	Text string
}

// Source searches posts.
type Source interface {
	// Search returns posts mentioning term posted since then (a source
	// that can't filter by time may return older ones; they are skipped).
	Search(ctx context.Context, term string, since time.Time) ([]Result, error)
}

// ErrSearchNotAvailable is what the Facebook source returns until its
// client is built: whether logged-out Facebook search can serve recent
// posts is not confirmed yet.
var ErrSearchNotAvailable = errors.New("discovery: Facebook search is not available yet")

// Unavailable is the Facebook source until its client exists. It sends
// nothing.
type Unavailable struct{}

func (Unavailable) Search(context.Context, string, time.Time) ([]Result, error) {
	return nil, ErrSearchNotAvailable
}
