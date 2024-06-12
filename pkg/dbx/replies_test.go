package dbx

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDBxSelectRepliesFromActorIds(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor1 := d.CreateActor()
	actor2 := d.CreateActor()
	ignored := d.CreateActor()
	actorids := []int64{actor1.ActorId, actor2.ActorId}
	root := d.CreatePost(&TestPostRefInput{Actor: ignored.Did})

	replies := make([]*PostRow, 0)
	replies = append(replies, d.CreatePost(&TestPostRefInput{Actor: actor1.Did, Reply: root.Uri}))
	replies = append(replies, d.CreatePost(&TestPostRefInput{Actor: actor2.Did, Reply: root.Uri}))
	d.CreatePost(&TestPostRefInput{Actor: ignored.Did, Reply: root.Uri})
	replies = append(replies, d.CreatePost(&TestPostRefInput{Actor: actor1.Did, Reply: root.Uri}))
	replies = append(replies, d.CreatePost(&TestPostRefInput{Actor: actor2.Did, Reply: root.Uri}))
	d.CreatePost(&TestPostRefInput{Actor: ignored.Did, Reply: root.Uri})
	slices.Reverse(replies)

	only, err := d.Replies.SelectRepliesFromActorIds(actorids, SQLiteMaxInt, 2)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		CollectPostIds(replies[0:2]),
		only,
	)

	only, err = d.Replies.SelectRepliesFromActorIds(actorids, replies[1].PostId, 2)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		CollectPostIds(replies[2:4]),
		only,
	)
}
