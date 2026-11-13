package stage

import "testing"

func TestCaptionText(t *testing.T) {
	srt := "\ufeff1\r\n00:00:00,000 --> 00:00:02,500\r\nFirst line\r\n\r\n2\r\n00:00:02,500 --> 00:00:05,000\r\n<i>Second</i> line\r\nSecond line\r\n"
	if got := captionText([]byte(srt)); got != "First line\nSecond line" {
		t.Fatalf("srt: %q", got)
	}
	vtt := "WEBVTT\n\nNOTE synthetic\n\n00:00.000 --> 00:02.000 align:start\n<c.yellow>One</c>\n\n00:02.000 --> 00:04.000\nTwo\n"
	if got := captionText([]byte(vtt)); got != "One\nTwo" {
		t.Fatalf("vtt: %q", got)
	}
	if got := captionText([]byte("<!DOCTYPE html><html><body>Log in to continue</body></html>")); got != "" {
		t.Fatalf("a login page read as captions: %q", got)
	}
}
