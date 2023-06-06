package dbx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDBxUpdateFollowsIndex(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	follower := d.CreateActor()
	follow := d.CreateFollow(follower, actor)

	assert.Equal(
		t,
		[]int64{},
		QueryPks(d.FollowsIndexed),
	)

	d.FollowsIndexed.FindOrCreateByActorId(actor.ActorId)
	d.FollowsIndexed.FindOrCreateByActorId(follower.ActorId)
	d.FollowsIndexed.SetLastFollow(actor.ActorId, follow.FollowId)
	d.FollowsIndexed.SetLastFollow(follower.ActorId, follow.FollowId)

	follow = d.CreateFollow(actor, follower)

	index, err := d.FollowsIndexed.FindByActorId(actor.ActorId)
	if err != nil {
		panic(err)
	}

	assert.Equal(
		t,
		follow.FollowId,
		index.LastFollow,
	)
}

func TestDBxSelectFollowsCreatesUnindexed(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	_, err := d.FollowsIndexed.SelectUnindexed(10)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]int64{},
		QueryPks(d.FollowsIndexed),
	)

	d.SelectMentionsFollowed(SQLiteMaxInt, 10, actor.Did)
	_, err = d.FollowsIndexed.SelectUnindexed(10)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]int64{1},
		QueryPks(d.FollowsIndexed),
	)
}
