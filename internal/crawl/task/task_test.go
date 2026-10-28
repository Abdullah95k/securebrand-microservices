package task

import (
	"testing"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
	taskv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fbs/task/v1"
)

func TestNormalizeRef(t *testing.T) {
	cases := map[string]string{
		"https://www.facebook.com/Nike":                    "v:nike",
		"https://m.facebook.com/nike/posts/123":            "v:nike",
		"facebook.com/profile.php?id=100064&sk=about":      "id:100064",
		"https://www.facebook.com/pages/Some-Name/1234567": "id:1234567",
		"100064000000000":                                  "id:100064000000000",
		"brand.official":                                   "v:brand.official",
		"v:already":                                        "v:already",
	}
	for in, want := range cases {
		got, err := NormalizeRef(in)
		if err != nil || got != want {
			t.Errorf("NormalizeRef(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "https://www.facebook.com/", "nike page!"} {
		if _, err := NormalizeRef(bad); err == nil {
			t.Errorf("NormalizeRef(%q) should fail", bad)
		}
	}
}

func TestDeterministicIDs(t *testing.T) {
	a := NewPage("job1", taskv1.Lane_LANE_P0, fbv1.CrawlMode_CRAWL_MODE_FULL, "v:nike", 0, "", &taskv1.Limits{})
	b := NewPage("job1", taskv1.Lane_LANE_P0, fbv1.CrawlMode_CRAWL_MODE_FULL, "v:nike", 0, "", &taskv1.Limits{})
	if a.TaskId != b.TaskId || a.LineageId != b.LineageId {
		t.Fatal("same inputs must give the same ids")
	}
	c := Continuation(a, "cur")
	if c.TaskId == a.TaskId || c.LineageId != a.LineageId || c.Seq != 1 {
		t.Fatalf("continuation ids wrong: %+v", c)
	}
	if Continuation(a, "cur").TaskId != c.TaskId {
		t.Fatal("continuations must be deterministic")
	}
	r := Retry(a, "")
	if r.TaskId != a.TaskId || r.Attempt != 1 {
		t.Fatal("retry keeps the task id")
	}
}
