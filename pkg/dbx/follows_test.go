package dbx

import (
	"fmt"
	"math/rand"
	"testing"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/flicknow/go-bluesky-bot/pkg/firehose"
	"github.com/flicknow/go-bluesky-bot/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func (d *testDBx) CreateFollow(actor *ActorRow, subject *ActorRow) *FollowRow {
	uri := fmt.Sprintf("at://%s/app.bsky.graph.follow/%d", actor.Did, rand.Int63())
	err := d.InsertFollow(&firehose.FollowRef{
		Subject: subject.Did,
		Ref:     &comatproto.RepoStrongRef{Uri: uri},
	})
	if err != nil {
		panic(err)
	}

	follow, err := d.Follows.FindByRkey(actor.ActorId, utils.ParseRkey(uri))
	if err != nil {
		panic(err)
	}

	return follow
}

func TestDBxInsertFollow(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	follower := d.CreateActor()
	follow := d.CreateFollow(follower, actor)

	assert.Equal(
		t,
		[]int64{follow.FollowId},
		QueryPks(d.Follows),
	)

	uri := fmt.Sprintf("at://%s/app.bsky.graph.follow/%s", follower.Did, follow.Rkey)
	d.DeleteFollow(uri)
	assert.Equal(
		t,
		[]int64{},
		QueryPks(d.Follows),
	)
}

/*
func TestDBxSelectAllMentionsFollowed(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	followed := d.CreateActor()
	unfollowed := d.CreateActor()

	_, err := d.FollowsIndexed.FindOrCreateByActorId(actor.ActorId)
	if err != nil {
		panic(err)
	}
	follow := d.CreateFollow(actor, followed)
	err = d.FollowsIndexed.SetLastFollow(actor.ActorId, follow.FollowId)
	if err != nil {
		panic(err)
	}

	post := d.CreatePost(&TestPostRefInput{Actor: actor.Did})
	seen := d.CreatePost(&TestPostRefInput{Actor: followed.Did, Reply: post.Uri})
	d.CreatePost(&TestPostRefInput{Actor: unfollowed.Did, Reply: post.Uri})

	posts, err := d.SelectAllMentionsFollowed(SQLiteMaxInt, 10, actor.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]int64{seen.PostId},
		CollectPostIds(posts),
	)
}
*/

func TestDBxSelectMentionsFollowed(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	followed := d.CreateActor()
	unfollowed := d.CreateActor()

	_, err := d.FollowsIndexed.FindOrCreateByActorId(actor.ActorId)
	if err != nil {
		panic(err)
	}
	follow := d.CreateFollow(actor, followed)
	err = d.FollowsIndexed.SetLastFollow(actor.ActorId, follow.FollowId)
	if err != nil {
		panic(err)
	}

	post := d.CreatePost(&TestPostRefInput{Actor: actor.Did})
	seen := d.CreatePost(&TestPostRefInput{Actor: followed.Did, Reply: post.Uri})
	d.CreatePost(&TestPostRefInput{Actor: unfollowed.Did, Reply: post.Uri})

	posts, err := d.SelectMentionsFollowed(SQLiteMaxInt, 10, actor.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]int64{seen.PostId},
		CollectPostIds(posts),
	)
}

func TestDBxSelectPostsByLabelsFollowed(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	followed := d.CreateActor()
	unfollowed := d.CreateActor()
	follow := d.CreateFollow(actor, followed)
	err := d.FollowsIndexed.SetLastFollow(actor.ActorId, follow.FollowId)
	if err != nil {
		panic(err)
	}

	seen := d.CreatePost(&TestPostRefInput{Actor: followed.Did}, "a")
	d.CreatePost(&TestPostRefInput{Actor: unfollowed.Did}, "a")

	found, err := d.SelectPostsByLabelsFollowed(SQLiteMaxInt-1, 10, actor.Did, "a")
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]int64{seen.PostId},
		CollectPostIds(found),
	)
}
