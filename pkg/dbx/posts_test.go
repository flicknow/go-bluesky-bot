package dbx

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDBxSelectPostsByActorIds(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor1 := d.CreateActor()
	actor2 := d.CreateActor()
	ignored := d.CreateActor()
	actorids := []int64{actor1.ActorId, actor2.ActorId}

	posts := make([]*PostRow, 0)
	posts = append(posts, d.CreatePost(&TestPostRefInput{Actor: actor1.Did}))
	posts = append(posts, d.CreatePost(&TestPostRefInput{Actor: actor2.Did}))
	d.CreatePost(&TestPostRefInput{Actor: ignored.Did})
	posts = append(posts, d.CreatePost(&TestPostRefInput{Actor: actor1.Did}))
	posts = append(posts, d.CreatePost(&TestPostRefInput{Actor: actor2.Did}))
	d.CreatePost(&TestPostRefInput{Actor: ignored.Did})
	slices.Reverse(posts)

	only, err := d.Posts.SelectPostsByActorIds(actorids, SQLiteMaxInt, 2)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		CollectPostIds(posts[0:2]),
		CollectPostIds(only),
	)

	only, err = d.Posts.SelectPostsByActorIds(actorids, only[len(only)-1].PostId, 2)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		CollectPostIds(posts[2:4]),
		CollectPostIds(only),
	)
}
