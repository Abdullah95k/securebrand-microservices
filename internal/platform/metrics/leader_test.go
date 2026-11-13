package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// A former leader's gauges go when it loses the lease: no series of the
// list, the queue or the schedule stays to be summed with the new leader's.
func TestResetLeaderGauges(t *testing.T) {
	MentionQueue.WithLabelValues("page").Set(19)
	ScheduledKinds.WithLabelValues("mention", "page").Set(71)
	PageTier.WithLabelValues("T1").Set(393)
	ConsumerLag.WithLabelValues("g", "t").Set(5)
	MentionChecksUsed.Set(432)
	AIMDCeiling.Set(2.5)
	ResetLeaderGauges()
	for name, n := range map[string]int{
		"queue":     testutil.CollectAndCount(MentionQueue),
		"kinds":     testutil.CollectAndCount(ScheduledKinds),
		"tiers":     testutil.CollectAndCount(PageTier),
		"lag":       testutil.CollectAndCount(ConsumerLag),
		"accounts":  testutil.CollectAndCount(MentionAccounts),
		"sched_lag": testutil.CollectAndCount(ScheduleLag),
	} {
		if n != 0 {
			t.Errorf("%s: %d series left", name, n)
		}
	}
	if v := testutil.ToFloat64(MentionChecksUsed); v != 0 {
		t.Errorf("checks used %v", v)
	}
	if v := testutil.ToFloat64(AIMDCeiling); v != 0 {
		t.Errorf("AIMD ceiling %v", v)
	}
}
