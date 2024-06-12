package dbx

import (
	"context"
	"testing"
	"time"

	"github.com/flicknow/go-bluesky-bot/pkg/clock"
	"github.com/flicknow/go-bluesky-bot/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func CollectSubjectIds(labels []*CustomLabel, neg bool) []int64 {
	subjectids := make([]int64, 0, len(labels))
	for _, label := range labels {
		if neg && (label.Neg == 0) {
			continue
		} else if !neg && (label.Neg != 0) {
			continue
		}
		subjectids = append(subjectids, label.SubjectId)
	}
	return subjectids
}

func TestDBxRecordBirthdayLabels(t *testing.T) {
	clock := clock.NewMockClock()

	ctx := context.WithValue(context.Background(), "clock", clock)

	d, cleanup := NewTestDBxContext(ctx)
	defer cleanup()

	now := time.Unix(clock.NowUnix(), 0)
	birthday := now.AddDate(-1, 0, 0).Add(5 * time.Minute)

	actor := d.CreateActor()
	_, err := d.Actors.InitializeBirthday(actor.Did, birthday.Unix())
	if err != nil {
		panic(err)
	}

	label, err := d.Labels.FindOrCreateLabel("birthday")
	if err != nil {
		panic(err)
	}

	err = d.RecordBirthdayLabels(clock)
	if err != nil {
		panic(err)
	}
	labels, err := d.CustomLabels.SelectLabelsByLabelId(label.LabelId, 0, 10)
	assert.Equal(
		t,
		[]int64{},
		CollectSubjectIds(labels, false),
	)

	clock.SetNow(now.Unix() + 600)
	err = d.RecordBirthdayLabels(clock)
	if err != nil {
		panic(err)
	}
	labels, _ = d.CustomLabels.SelectLabelsByLabelId(label.LabelId, 0, 10)
	assert.Equal(
		t,
		[]int64{actor.ActorId},
		CollectSubjectIds(labels, false),
	)
}

func TestDBxRecordUnbirthdayLabels(t *testing.T) {
	clock := clock.NewMockClock()

	ctx := context.WithValue(context.Background(), "clock", clock)

	d, cleanup := NewTestDBxContext(ctx)
	defer cleanup()

	now := time.Unix(clock.NowUnix(), 0)
	birthday := now.AddDate(-1, 0, -1).Add(5 * time.Minute)

	actor := d.CreateActor()
	_, err := d.Actors.InitializeBirthday(actor.Did, birthday.Unix())
	if err != nil {
		panic(err)
	}

	label, err := d.Labels.FindOrCreateLabel("birthday")
	if err != nil {
		panic(err)
	}

	err = d.RecordUnbirthdayLabels(clock)
	if err != nil {
		panic(err)
	}
	labels, _ := d.CustomLabels.SelectLabelsByLabelId(label.LabelId, 0, 10)
	assert.Equal(
		t,
		[]int64{},
		CollectSubjectIds(labels, true),
	)

	clock.SetNow(now.Unix() + 600)
	err = d.RecordUnbirthdayLabels(clock)
	if err != nil {
		panic(err)
	}
	labels, _ = d.CustomLabels.SelectLabelsByLabelId(label.LabelId, 0, 10)
	assert.Equal(
		t,
		[]int64{actor.ActorId},
		CollectSubjectIds(labels, true),
	)
	assert.Equal(
		t,
		[]int64{},
		CollectSubjectIds(labels, false),
	)
}

func TestDBxTestOllieBangerCustomLabels(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	ollie, err := d.Actors.FindOrCreateActor(ʕ٠ᴥ٠ʔ)
	if err != nil {
		panic(err)
	}

	post := d.CreatePost(&TestPostRefInput{Actor: actor.Did})
	reply := d.CreatePost(&TestPostRefInput{Actor: ollie.Did, Reply: post.Uri, Text: "ℹ️banger"})

	banger, err := d.Labels.FindOrCreateLabel("banger")
	if err != nil {
		panic(err)
	}

	labels, _ := d.CustomLabels.SelectLabelsByLabelId(banger.LabelId, 0, 10)
	assert.Equal(
		t,
		[]int64{post.PostId},
		CollectSubjectIds(labels, false),
	)

	d.DeletePost(reply.Uri)
}

func TestDBxTestBangerCustomLabels(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	mark, err := d.Actors.FindOrCreateActor(MARK)
	if err != nil {
		panic(err)
	}

	post := d.CreatePost(&TestPostRefInput{Actor: actor.Did})
	like := d.CreateLike(mark, post)

	banger, err := d.Labels.FindOrCreateLabel("banger")
	if err != nil {
		panic(err)
	}

	labels, _ := d.CustomLabels.SelectLabelsByLabelId(banger.LabelId, 0, 10)
	assert.Equal(
		t,
		[]int64{post.PostId},
		CollectSubjectIds(labels, false),
	)

	err = d.DeleteLike(utils.HydrateUri(like.DehydratedUri, "app.bsky.feed.like"))
	if err != nil {
		panic(err)
	}

	labels, _ = d.CustomLabels.SelectLabelsByLabelId(banger.LabelId, 0, 10)
	assert.Equal(
		t,
		[]int64{post.PostId},
		CollectSubjectIds(labels, true),
	)
}

func TestDBxSelectBirthdays(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	clock := d.clock
	nobodyBoy := d.CreateActor()
	now := time.Unix(clock.NowUnix(), 0)
	birthday := now.AddDate(-1, 0, 0).Add(-1 * time.Minute)
	birthdayBoy := d.CreateActor()
	_, err := d.Actors.InitializeBirthday(birthdayBoy.Did, birthday.Unix())
	if err != nil {
		panic(err)
	}

	err = d.RecordBirthdayLabels(clock)
	if err != nil {
		panic(err)
	}

	birthdayPost := d.CreatePost(&TestPostRefInput{Actor: birthdayBoy.Did})
	d.CreatePost(&TestPostRefInput{Actor: nobodyBoy.Did})

	found, err := d.SelectBirthdays(SQLiteMaxInt-1, 10)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]int64{birthdayPost.PostId},
		CollectPostIds(found),
	)
}

func TestDBxSelectBirthdaysFollowed(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	birthdayFollowed := d.CreateActor()
	nobodyFollowed := d.CreateActor()
	birthdayUnfollowed := d.CreateActor()
	nobodyUnfollowed := d.CreateActor()
	d.CreateFollow(actor, birthdayFollowed)
	follow := d.CreateFollow(actor, nobodyFollowed)

	_, err := d.FollowsIndexed.FindOrCreateByActorId(actor.ActorId)
	if err != nil {
		panic(err)
	}
	err = d.FollowsIndexed.SetLastFollow(actor.ActorId, follow.FollowId)
	if err != nil {
		panic(err)
	}

	clock := d.clock
	now := time.Unix(clock.NowUnix(), 0)
	birthday := now.AddDate(-1, 0, 0).Add(-1 * time.Minute)
	_, err = d.Actors.InitializeBirthday(birthdayFollowed.Did, birthday.Unix())
	if err != nil {
		panic(err)
	}
	_, err = d.Actors.InitializeBirthday(birthdayUnfollowed.Did, birthday.Unix())
	if err != nil {
		panic(err)
	}
	err = d.RecordBirthdayLabels(clock)
	if err != nil {
		panic(err)
	}

	seen := d.CreatePost(&TestPostRefInput{Actor: birthdayFollowed.Did})
	d.CreatePost(&TestPostRefInput{Actor: nobodyFollowed.Did})
	d.CreatePost(&TestPostRefInput{Actor: birthdayUnfollowed.Did})
	d.CreatePost(&TestPostRefInput{Actor: nobodyUnfollowed.Did})

	found, err := d.SelectBirthdaysFollowed(SQLiteMaxInt-1, 10, actor.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]int64{seen.PostId},
		CollectPostIds(found),
	)
}
