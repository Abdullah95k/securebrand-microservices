package parse

import "testing"

func TestParseCount(t *testing.T) {
	for in, want := range map[string]int64{
		"1,234 followers": 1234, "1.2M followers": 1_200_000, "980K likes": 980_000,
		"3,4K": 3400, "12": 12, "2.5B": 2_500_000_000,
	} {
		got, ok := ParseCount(in)
		if !ok || got != want {
			t.Errorf("ParseCount(%q) = %d, %v; want %d", in, got, ok, want)
		}
	}
}

func TestGraphIDs(t *testing.T) {
	if got := CommentIDFromGraphID("Y29tbWVudDo1NTVfODg4"); got != 888 { // comment:555_888
		t.Fatalf("comment id = %d", got)
	}
	if got := PostIDFromStoryID("UzpfSTEwMDo1NTU6NTU1"); got != 555 { // S:_I100:555:555
		t.Fatalf("post id = %d", got)
	}
	if FeedbackID(5) != "ZmVlZGJhY2s6NQ==" {
		t.Fatalf("feedback id = %s", FeedbackID(5))
	}
	first, again, other := MediaID("1", "sd"), MediaID("1", "sd"), MediaID("1", "hd")
	if first != again || first == other {
		t.Fatal("media ids must be deterministic per variant")
	}
}

func TestURLExpiry(t *testing.T) {
	ts := URLExpiry("https://scontent.xx.fbcdn.net/v/x.jpg?stp=dst&oe=6A1B2C3D&oh=00_abc")
	if ts == nil || ts.AsTime().Unix() != 0x6A1B2C3D {
		t.Fatalf("expiry = %v", ts)
	}
	if URLExpiry("https://example.com/x.jpg") != nil {
		t.Fatal("no oe should give nil")
	}
}
