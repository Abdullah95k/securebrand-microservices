package persist

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// Only errors a retry can never fix count as a refused record: a race
// between writers (unique violation) or a partition not created yet (check
// violation, joined with the reason creating it failed) are retried.
func TestIsDataError(t *testing.T) {
	pg := func(code string) error { return &pgconn.PgError{Code: code} }
	for err, want := range map[error]bool{
		pg("22023"): true,  // a refused value
		pg("22P05"): true,  // a NUL character
		pg("23502"): true,  // a NOT NULL field missing
		pg("54000"): true,  // a value too large for its index
		pg("23505"): false, // two writers racing for a unique value
		pg("23514"): false, // no partition for the row
		errors.Join(pg("23514"), errors.New("lock timeout")): false,
		pg("40001"):                    false,
		errors.New("connection reset"): false,
	} {
		if got := isDataError(err); got != want {
			t.Errorf("%v: %v, want %v", err, got, want)
		}
	}
}
