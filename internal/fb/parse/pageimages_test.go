package parse

import (
	"testing"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
)

// A page's pictures keep their media IDs while the picture is the same,
// however its signed link changes, and get new ones when it changes.
func TestPageImagesFollowThePicture(t *testing.T) {
	page := func(pic, cover string) *fbv1.Page {
		return &fbv1.Page{PageId: 7, ProfilePicture: &fbv1.ImageRef{Url: pic}, CoverPhoto: &fbv1.ImageRef{Url: cover}}
	}
	a := PageImages(page("https://scontent.example/v/t39/111_222_n.jpg?oe=AAAA&oh=x", "https://scontent.example/v/t39/333_444_n.jpg?oe=AAAA"))
	b := PageImages(page("https://scontent.example/v/t39/111_222_n.jpg?oe=BBBB&oh=y", "https://scontent.example/v/t39/333_444_n.jpg?oe=CCCC"))
	c := PageImages(page("https://scontent.example/v/t39/555_666_n.jpg?oe=BBBB", "https://scontent.example/v/t39/333_444_n.jpg?oe=CCCC"))
	if len(a) != 2 || a[0].Kind != fbv1.MediaKind_MEDIA_KIND_PROFILE_PIC || a[1].Kind != fbv1.MediaKind_MEDIA_KIND_COVER ||
		a[0].OwnerType != "page" || a[0].OwnerId != 7 || a[0].PageId != 7 {
		t.Fatalf("%+v", a)
	}
	if a[0].MediaId != b[0].MediaId || a[1].MediaId != b[1].MediaId {
		t.Fatal("the same picture under a new signed link got a new media ID")
	}
	if c[0].MediaId == a[0].MediaId || c[1].MediaId != a[1].MediaId {
		t.Fatal("a changed profile picture kept its media ID, or an unchanged cover lost it")
	}
	if got := PageImages(&fbv1.Page{PageId: 7}); len(got) != 0 {
		t.Fatalf("a page without pictures: %+v", got)
	}
}
