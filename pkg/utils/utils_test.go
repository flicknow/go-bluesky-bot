package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexN(t *testing.T) {
	assert.Equal(t, []int{}, indexN("", "/", 0))
	assert.Equal(t, []int{}, indexN("", "/", 1))
	assert.Equal(t, []int{}, indexN("a", "/", 1))
	assert.Equal(t, []int{}, indexN("foo/bar", "/", 0))
	assert.Equal(t, []int{3}, indexN("foo/bar", "/", 1))
	assert.Equal(t, []int{3}, indexN("foo/bar", "/", 2))
	assert.Equal(t, []int{3, 7}, indexN("foo/bar/", "/", 2))
	assert.Equal(t, []int{3, 7}, indexN("foo/bar/baz", "/", 2))
	assert.Equal(t, []int{3, 7}, indexN("foo/bar/baz", "/", 3))
	assert.Equal(t, []int{0, 4}, indexN("/foo/bar/baz", "/", 2))
	assert.Equal(t, []int{0, 4, 8}, indexN("/foo/bar/baz", "/", 3))
}

func TestDehydrateUri(t *testing.T) {
	assert.Equal(t, "foo", DehydrateUri("foo"))
	assert.Equal(t, "at://foo/app.bsky.feed.post/bar", DehydrateUri("at://foo/app.bsky.feed.post/bar"))
	assert.Equal(t, "plc:oyflxtg6rywzq5zguee5pqyi/3kfsys63bfk2n", DehydrateUri("at://did:plc:oyflxtg6rywzq5zguee5pqyi/app.bsky.feed.post/3kfsys63bfk2n"))
}

func TestHydrateUri(t *testing.T) {
	assert.Equal(t, "at://did:plc:oyflxtg6rywzq5zguee5pqyi/app.bsky.feed.post/3kfsys63bfk2n", HydrateUri("plc:oyflxtg6rywzq5zguee5pqyi/3kfsys63bfk2n", "app.bsky.feed.post"))

	tests := []string{
		"foo",
		"at://foo/app.bsky.feed.post/bar",
		"at://did:plc:m26cjl666rhi4bie4alua3vq/app.bsky.feed.post/3kfr7bjxgqk2o",
	}
	for _, test := range tests {
		assert.Equal(
			t,
			test,
			HydrateUri(DehydrateUri(test), "app.bsky.feed.post"),
		)
	}

}

func TestParseRkey(t *testing.T) {
	assert.Equal(
		t,
		"3kdwia2fiv32l",
		ParseRkey("at://did:plc:l5riuaghvetqxnfxzen5a7hn/app.bsky.graph.follow/3kdwia2fiv32l"),
	)
}
