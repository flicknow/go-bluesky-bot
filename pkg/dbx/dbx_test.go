package dbx

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"testing"

	"github.com/flicknow/go-bluesky-bot/pkg/clock"
	"github.com/stretchr/testify/assert"
)

func TestDBxInsertPost(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	uri := NewTestPostUri(actor.Did)
	post := d.CreatePost(&TestPostRefInput{Uri: uri})

	assert.Equal(t, uri, post.Uri, "indexed post uri")
	assert.Equal(t, actor.ActorId, post.ActorId, "indexed post actor")
}

func TestDBxInsertReply(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	op := d.CreatePost(&TestPostRefInput{Actor: actor.Did})

	replyGuy := d.CreateActor()
	reply := d.CreatePost(&TestPostRefInput{Actor: replyGuy.Did, Reply: op.Uri})

	replyRow, err := d.Replies.FindByPostId(reply.PostId)
	if err != nil {
		panic(err)
	}

	assert.Equal(t, op.ActorId, replyRow.ParentActorId, "indexed parent actor")
	assert.Equal(t, op.PostId, replyRow.ParentId, "indexed parent post")

	d.DeletePost(reply.Uri)
	_, err = d.Replies.FindByPostId(reply.PostId)
	assert.True(t, errors.Is(err, sql.ErrNoRows))
}

func TestDBxInsertQuote(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	op := d.CreatePost(&TestPostRefInput{Actor: actor.Did})

	quoteGuy := d.CreateActor()
	quote := d.CreatePost(&TestPostRefInput{Actor: quoteGuy.Did, Quote: op.Uri})

	quoteRow, err := d.Quotes.FindByPostId(quote.PostId)
	if err != nil {
		panic(err)
	}

	assert.Equal(t, op.ActorId, quoteRow.SubjectActorId, "indexed quoted actor")
	assert.Equal(t, op.PostId, quoteRow.SubjectId, "indexed quoted post")

	d.DeletePost(quote.Uri)
	_, err = d.Replies.FindByPostId(quote.PostId)
	assert.True(t, errors.Is(err, sql.ErrNoRows))
}

func TestDBxInsertMentions(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	uri := NewTestPostUri(actor.Did)

	mentionedActors := make([]*ActorRow, 3)
	for i := 0; i < 3; i++ {
		mentionedActors[i] = d.CreateActor()
	}

	mentionedDids := make([]string, len(mentionedActors))
	for i := range mentionedActors {
		mentionedDids[i] = mentionedActors[i].Did
	}

	mentionedActorIds := make([]int64, len(mentionedActors))
	for i := range mentionedActors {
		mentionedActorIds[i] = mentionedActors[i].ActorId
	}

	postRow := d.CreatePost(&TestPostRefInput{Mentions: mentionedDids, Uri: uri})
	foundActorIds, err := d.Mentions.SelectMentions(postRow.PostId)
	if err != nil {
		panic(err)
	}

	assert.Equal(t, mentionedActorIds, foundActorIds, "indexed mentioned actor")

	d.DeletePost(postRow.Uri)
	foundActorIds, err = d.Mentions.SelectMentions(postRow.PostId)
	if err != nil {
		panic(err)
	}
	assert.Empty(t, foundActorIds)
}

func TestDBxInsertPostLabels(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	uri := NewTestPostUri(actor.Did)

	labelNames := []string{"foo", "bar", "baz"}
	labelRows := make([]*LabelRow, len(labelNames))
	for i := range labelNames {
		labelRow, err := d.Labels.FindOrCreateLabel(labelNames[i])
		if err != nil {
			panic(err)
		}
		labelRows[i] = labelRow
	}

	for i := range labelNames {
		labelRow, err := d.Labels.FindOrCreateLabel(labelNames[i])
		if err != nil {
			panic(err)
		}
		labelRows[i] = labelRow
	}

	labelIds := make([]int64, len(labelNames))
	for i, labelRow := range labelRows {
		labelIds[i] = labelRow.LabelId
	}

	postRow := d.CreatePost(&TestPostRefInput{Uri: uri}, labelNames...)
	foundLabelIds, err := d.PostLabels.SelectLabelsByPostId(postRow.PostId)
	if err != nil {
		panic(err)
	}

	assert.Equal(t, labelIds, foundLabelIds, "indexed labels found for post")

	d.DeletePost(postRow.Uri)
	foundLabelIds, err = d.PostLabels.SelectLabelsByPostId(postRow.PostId)
	if err != nil {
		panic(err)
	}
	assert.Empty(t, foundLabelIds)
}
func TestDBxInsertDms(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	op := d.CreatePost(&TestPostRefInput{Actor: actor.Did})

	quotee := d.CreateActor()
	quoted := d.CreatePost(&TestPostRefInput{Actor: quotee.Did})

	friend := d.CreateActor()

	dmer := d.CreateActor()
	dm := d.CreatePost(&TestPostRefInput{Actor: dmer.Did, Mentions: []string{friend.Did}, Quote: quoted.Uri, Reply: op.Uri, Text: "DM: @my.friend"})
	dmreply := d.CreatePost(&TestPostRefInput{Actor: friend.Did, Reply: dm.Uri})

	dmerDms, err := d.SelectDms(SQLiteMaxInt, 100, dmer.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]int64{dmreply.PostId, dm.PostId},
		CollectPostIds(dmerDms),
	)

	friendDms, err := d.SelectDms(SQLiteMaxInt, 100, friend.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]int64{dmreply.PostId, dm.PostId},
		CollectPostIds(friendDms),
	)

	opDms, err := d.SelectDms(SQLiteMaxInt, 100, actor.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]*PostRow{},
		opDms,
	)

	quoteeDms, err := d.SelectDms(SQLiteMaxInt, 100, quotee.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]*PostRow{},
		quoteeDms,
	)

	d.DeletePost(dm.Uri)
	deleted, err := d.Dms.SelectDms(dm.PostId)
	if err != nil {
		panic(err)
	}
	assert.Empty(t, deleted)
}

func TestDBxInsertThreadMentions(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	op := d.CreatePost(&TestPostRefInput{Actor: actor.Did})

	quotee := d.CreateActor()
	quoted := d.CreatePost(&TestPostRefInput{Actor: quotee.Did})

	mentioned := d.CreateActor()

	replyGuy := d.CreateActor()
	reply := d.CreatePost(&TestPostRefInput{Actor: replyGuy.Did, Mentions: []string{mentioned.Did}, Quote: quoted.Uri, Reply: op.Uri})

	mentionedActors, err := d.ThreadMentions.SelectThreadMentions(reply.PostId)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		SortInt64s([]int64{actor.ActorId, mentioned.ActorId, quotee.ActorId}),
		SortInt64s(mentionedActors),
		"indexed parent, mentioned, and quoted actors",
	)

	opMentions, err := d.ThreadMentions.SelectThreadMentionsByActorId(actor.ActorId, SQLiteMaxInt, 10)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]int64{reply.PostId},
		opMentions,
		"replies show up in mentions",
	)

	quoteeMentions, err := d.ThreadMentions.SelectThreadMentionsByActorId(quotee.ActorId, SQLiteMaxInt, 10)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]int64{reply.PostId},
		quoteeMentions,
		"quotes show up in mentions",
	)

	mentionedMentions, err := d.ThreadMentions.SelectThreadMentionsByActorId(mentioned.ActorId, SQLiteMaxInt, 10)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]int64{reply.PostId},
		mentionedMentions,
		"mention shows up in mentions",
	)

	replyGuyMentions, err := d.ThreadMentions.SelectThreadMentionsByActorId(replyGuy.ActorId, SQLiteMaxInt, 10)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]int64{},
		replyGuyMentions,
		"own post does not show up in mentions",
	)

	d.CreatePost(&TestPostRefInput{Actor: actor.Did, Reply: op.Uri})
	opMentions, err = d.ThreadMentions.SelectThreadMentionsByActorId(actor.ActorId, SQLiteMaxInt, 10)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]int64{reply.PostId},
		opMentions,
		"replying to yourself does not show up in mentions",
	)

	d.CreatePost(&TestPostRefInput{Actor: actor.Did, Quote: op.Uri})
	opMentions, err = d.ThreadMentions.SelectThreadMentionsByActorId(actor.ActorId, SQLiteMaxInt, 10)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]int64{reply.PostId},
		opMentions,
		"quoting yourself does not show up in mentions",
	)

	d.DeletePost(reply.Uri)
	deleted, err := d.ThreadMentions.SelectThreadMentions(reply.PostId)
	if err != nil {
		panic(err)
	}
	assert.Empty(t, deleted)
}

func TestDBxSelectMentions(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	op := d.CreatePost(&TestPostRefInput{Actor: actor.Did})

	quotee := d.CreateActor()
	quoted := d.CreatePost(&TestPostRefInput{Actor: quotee.Did})

	mentioned := d.CreateActor()

	replyGuy := d.CreateActor()
	reply := d.CreatePost(&TestPostRefInput{Actor: replyGuy.Did, Mentions: []string{mentioned.Did}, Quote: quoted.Uri, Reply: op.Uri})

	opMentions, err := d.SelectMentions(SQLiteMaxInt, 10, actor.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]*PostRow{reply},
		opMentions,
		"replies show up in mentions",
	)

	replyMentions, err := d.SelectMentions(SQLiteMaxInt, 10, replyGuy.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]*PostRow{},
		replyMentions,
	)

	mentionedMentions, err := d.SelectMentions(SQLiteMaxInt, 10, mentioned.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]*PostRow{reply},
		mentionedMentions,
	)

	quoteeMentions, err := d.SelectAllMentions(SQLiteMaxInt, 10, quotee.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]*PostRow{reply},
		quoteeMentions,
		"quotes show up in mentions",
	)

	d.CreatePost(&TestPostRefInput{Actor: replyGuy.Did, Reply: reply.Uri})
	opMentions, err = d.SelectMentions(SQLiteMaxInt, 10, actor.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]int64{reply.PostId},
		CollectPostIds(opMentions),
		"indirect reply does not show up in mentions",
	)
}

func TestDBxSelectPostsByLabels(t *testing.T) {
	clock := clock.NewMockClock()
	ctx := context.WithValue(context.Background(), "clock", clock)

	d, cleanup := NewTestDBxContext(ctx)
	defer cleanup()

	actor := d.CreateActor()
	labelAs := []*PostRow{
		d.CreatePost(&TestPostRefInput{Actor: actor.Did}, "a"),
		d.CreatePost(&TestPostRefInput{Actor: actor.Did}, "a"),
	}
	labelBs := []*PostRow{
		d.CreatePost(&TestPostRefInput{Actor: actor.Did}, "b"),
		d.CreatePost(&TestPostRefInput{Actor: actor.Did}, "b"),
	}

	foundAs, err := d.SelectPostsByLabels(SQLiteMaxInt-1, 10, "a")
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		sortByPostIdDesc(labelAs),
		foundAs,
	)

	foundBs, err := d.SelectPostsByLabels(SQLiteMaxInt-1, 10, "b")
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		sortByPostIdDesc(labelBs),
		foundBs,
	)

	foundABs, err := d.SelectPostsByLabels(SQLiteMaxInt-1, 10, "a", "b")
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		sortByPostIdDesc(concatPosts(labelAs, labelBs)),
		foundABs,
	)

	foundNow, err := d.SelectPostsByLabels(SQLiteMaxInt, 10, "a")
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]*PostRow{},
		foundNow,
	)

	clock.SetNow(clock.NowUnix() + 1 + (5 * 60))
	foundLater, err := d.SelectPostsByLabels(SQLiteMaxInt, 10, "a")
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		sortByPostIdDesc(labelAs),
		foundLater,
	)
}

func TestDBxSelectAllMentions(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	op := d.CreatePost(&TestPostRefInput{Actor: actor.Did})

	quotee := d.CreateActor()
	quoted := d.CreatePost(&TestPostRefInput{Actor: quotee.Did})

	mentioned := d.CreateActor()

	replyGuy := d.CreateActor()
	reply := d.CreatePost(&TestPostRefInput{Mentions: []string{mentioned.Did}, Quote: quoted.Uri, Reply: op.Uri, Uri: NewTestPostUri(replyGuy.Did)})

	opMentions, err := d.SelectMentions(SQLiteMaxInt, 10, actor.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]*PostRow{reply},
		opMentions,
		"replies show up in mentions",
	)

	replyMentions, err := d.SelectMentions(SQLiteMaxInt, 10, replyGuy.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]*PostRow{},
		replyMentions,
	)

	mentionedMentions, err := d.SelectMentions(SQLiteMaxInt, 10, mentioned.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]*PostRow{reply},
		mentionedMentions,
	)

	quoteeMentions, err := d.SelectAllMentions(SQLiteMaxInt, 10, quotee.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]*PostRow{reply},
		quoteeMentions,
		"quotes show up in mentions",
	)

	indirectReply := d.CreatePost(&TestPostRefInput{Actor: replyGuy.Did, Reply: reply.Uri})
	opMentions, err = d.SelectAllMentions(SQLiteMaxInt, 10, actor.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		CollectPostIds([]*PostRow{indirectReply, reply}),
		CollectPostIds(opMentions),
		"indirect reply does shows up in mentions",
	)
}

func TestDBxActorPostCount(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	other := d.CreateActor()
	actor := d.CreateActor()

	otherPost := d.CreatePost(&TestPostRefInput{Actor: other.Did})

	reply := d.CreatePost(&TestPostRefInput{Actor: actor.Did, Reply: otherPost.Uri})
	actor, err := d.Actors.FindActor(actor.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, int64(0), actor.Posts)

	post := d.CreatePost(&TestPostRefInput{Actor: actor.Did})
	actor, err = d.Actors.FindActor(actor.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, int64(1), actor.Posts)

	err = d.DeletePost(reply.Uri)
	if err != nil {
		panic(err)
	}
	actor, err = d.Actors.FindActor(actor.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, int64(1), actor.Posts)

	err = d.DeletePost(post.Uri)
	if err != nil {
		panic(err)
	}
	actor, err = d.Actors.FindActor(actor.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, int64(0), actor.Posts)
}

func TestDBxSelectMark(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	mark, err := d.Actors.FindOrCreateActor(MARK)
	if err != nil {
		panic(err)
	}

	actor := d.CreateActor()

	post := d.CreatePost(&TestPostRefInput{Actor: actor.Did})
	reply := d.CreatePost(&TestPostRefInput{Actor: mark.Did, Reply: post.Uri})
	mention := d.CreatePost(&TestPostRefInput{Actor: mark.Did, Mentions: []string{actor.Did}})
	quote := d.CreatePost(&TestPostRefInput{Actor: mark.Did, Quote: post.Uri})

	other := d.CreateActor()
	otherPost := d.CreatePost(&TestPostRefInput{Actor: other.Did})
	d.CreatePost(&TestPostRefInput{Actor: mark.Did, Reply: otherPost.Uri})

	feed, err := d.SelectMark(SQLiteMaxInt, 10, actor.Did)
	if err != nil {
		panic(err)
	}

	assert.Equal(
		t,
		CollectPostIds([]*PostRow{quote, mention, reply}),
		CollectPostIds(feed),
		"returned quote mention and reply from mark",
	)
}

func TestDBxPrune(t *testing.T) {
	clock := clock.NewMockClock()
	ctx := context.WithValue(context.Background(), "clock", clock)

	d, cleanup := NewTestDBxContext(ctx)
	defer cleanup()

	actor := d.CreateActor()
	other := d.CreateActor()
	oldPost := d.CreatePost(&TestPostRefInput{Actor: other.Did}, "a")
	d.CreatePost(&TestPostRefInput{Actor: actor.Did, Mentions: []string{other.Did}, Quote: oldPost.Uri, Reply: oldPost.Uri, Text: "DM: @other.guy"})

	anotherOldPost := d.CreatePost(&TestPostRefInput{Actor: other.Did}, "a")
	d.CreatePost(&TestPostRefInput{Actor: actor.Did, Mentions: []string{other.Did}, Quote: anotherOldPost.Uri, Reply: anotherOldPost.Uri, Text: "DM: @other.guy"})
	d.CreateLike(actor, oldPost)
	d.CreateRepost(actor, oldPost)
	d.CreateLike(actor, anotherOldPost)
	d.CreateRepost(actor, anotherOldPost)

	now := clock.NowUnix() + 100
	clock.SetNow(now)

	newPost := d.CreatePost(&TestPostRefInput{Actor: other.Did}, "b")
	d.CreatePost(&TestPostRefInput{Actor: actor.Did, Mentions: []string{other.Did}, Quote: newPost.Uri, Reply: newPost.Uri, Text: "DM: @other.guy"})

	pruned, err := d.Prune(now-10, 2)
	if err != nil {
		panic(err)
	}

	assert.Equal(t, 2, pruned)

	assert.Equal(
		t,
		[]int64{3, 4, 5, 6},
		QueryPks(d.Posts),
	)
	assert.Equal(
		t,
		[]int64{2},
		QueryPks(d.Likes),
	)
	assert.Equal(
		t,
		[]int64{2},
		QueryPks(d.Reposts),
	)
	assert.Equal(
		t,
		[]int64{3, 4, 5, 6},
		QueryPks(d.Dms),
	)
	assert.Equal(
		t,
		[]int64{2, 3},
		QueryPks(d.Mentions),
	)

	assert.Equal(
		t,
		[]int64{2, 3},
		QueryPks(d.PostLabels),
	)
	assert.Equal(
		t,
		[]int64{2, 3},
		QueryPks(d.Quotes),
	)
	assert.Equal(
		t,
		[]int64{2, 3},
		QueryPks(d.Replies),
	)
	assert.Equal(
		t,
		[]int64{2, 3},
		QueryPks(d.ThreadMentions),
	)

	pruned, err = d.Prune(now-10, 2)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, pruned, 2)

	assert.Equal(
		t,
		[]int64{5, 6},
		QueryPks(d.Posts),
	)
	assert.Equal(
		t,
		[]int64{},
		QueryPks(d.Likes),
	)
	assert.Equal(
		t,
		[]int64{},
		QueryPks(d.Reposts),
	)
	assert.Equal(
		t,
		[]int64{5, 6},
		QueryPks(d.Dms),
	)
	assert.Equal(
		t,
		[]int64{3},
		QueryPks(d.Mentions),
	)

	assert.Equal(
		t,
		[]int64{3},
		QueryPks(d.PostLabels),
	)
	assert.Equal(
		t,
		[]int64{3},
		QueryPks(d.Quotes),
	)
	assert.Equal(
		t,
		[]int64{3},
		QueryPks(d.Replies),
	)
	assert.Equal(
		t,
		[]int64{3},
		QueryPks(d.ThreadMentions),
	)

	pruned, err = d.Prune(now-10, 2)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, pruned, 0)
}

func TestDBxSelectPostLabels(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	d.CreatePost(&TestPostRefInput{Actor: actor.Did}, "a")
	second := d.CreatePost(&TestPostRefInput{Actor: actor.Did}, "a")
	third := d.CreatePost(&TestPostRefInput{Actor: actor.Did}, "a")
	fourth := d.CreatePost(&TestPostRefInput{Actor: actor.Did}, "a")

	labels, err := d.SelectPostLabels(0, 2)
	if err != nil {
		panic(err)
	}

	assert.Equal(
		t,
		[]*LabelDef{
			{PostLabelId: 3, CreatedAt: third.CreatedAt, Uri: third.Uri, Val: "a"},
			{PostLabelId: 4, CreatedAt: fourth.CreatedAt, Uri: fourth.Uri, Val: "a"},
		},
		labels,
	)

	labels, err = d.SelectPostLabels(1, 2)
	if err != nil {
		panic(err)
	}

	assert.Equal(
		t,
		[]*LabelDef{
			{PostLabelId: 2, CreatedAt: second.CreatedAt, Uri: second.Uri, Val: "a"},
			{PostLabelId: 3, CreatedAt: third.CreatedAt, Uri: third.Uri, Val: "a"},
		},
		labels,
	)
}

func TestDBxDeleteLike(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	post := d.CreatePost(&TestPostRefInput{Actor: actor.Did})
	assert.Equal(
		t,
		int64(0),
		post.Likes,
	)

	like := d.CreateLike(actor, post)

	post, err := d.Posts.FindByUri(post.Uri)
	if err != nil {
		panic(err)
	}

	assert.Equal(
		t,
		int64(1),
		post.Likes,
	)

	d.DeleteLike(like.Uri)
	post, err = d.Posts.FindByUri(post.Uri)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		int64(0),
		post.Likes,
	)

	assert.Equal(
		t,
		[]int64{},
		QueryPks(d.Likes),
	)

	d.CreateLike(actor, post)
	assert.Equal(
		t,
		[]int64{1},
		QueryPks(d.Likes),
	)

	d.DeletePost(post.Uri)
	assert.Equal(
		t,
		[]int64{},
		QueryPks(d.Likes),
	)
}

func TestDBxDeleteRepost(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	post := d.CreatePost(&TestPostRefInput{Actor: actor.Did})
	assert.Equal(
		t,
		int64(0),
		post.Reposts,
	)

	repost := d.CreateRepost(actor, post)

	post, err := d.Posts.FindByUri(post.Uri)
	if err != nil {
		panic(err)
	}

	assert.Equal(
		t,
		int64(1),
		post.Reposts,
	)

	d.DeleteRepost(repost.Uri)
	post, err = d.Posts.FindByUri(post.Uri)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		int64(0),
		post.Reposts,
	)

	assert.Equal(
		t,
		[]int64{},
		QueryPks(d.Reposts),
	)

	d.CreateRepost(actor, post)
	assert.Equal(
		t,
		[]int64{1},
		QueryPks(d.Reposts),
	)

	d.DeletePost(post.Uri)
	assert.Equal(
		t,
		[]int64{},
		QueryPks(d.Reposts),
	)
}

func TestDBxInsertDeletedMentions(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	replyguy := d.CreateActor()

	deleted := NewTestPostUri(actor.Did)
	quote := d.CreatePost(&TestPostRefInput{Actor: replyguy.Did, Quote: deleted})
	reply := d.CreatePost(&TestPostRefInput{Actor: replyguy.Did, Reply: deleted})

	mentions, err := d.SelectMentions(SQLiteMaxInt, 10, actor.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		CollectPostIds([]*PostRow{reply, quote}),
		CollectPostIds(mentions),
	)

	threadmentions, err := d.SelectAllMentions(SQLiteMaxInt, 10, actor.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		CollectPostIds([]*PostRow{reply, quote}),
		CollectPostIds(threadmentions),
	)
}

func TestDBxSelectOnlyPosts(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()
	posts := make([]*PostRow, 0)

	posts = append(posts, d.CreatePost(&TestPostRefInput{Actor: actor.Did}))
	d.CreatePost(&TestPostRefInput{Actor: actor.Did, Reply: posts[len(posts)-1].Uri})
	d.CreatePost(&TestPostRefInput{Actor: actor.Did, Reply: posts[len(posts)-1].Uri})
	posts = append(posts, d.CreatePost(&TestPostRefInput{Actor: actor.Did}))
	d.CreatePost(&TestPostRefInput{Actor: actor.Did, Reply: posts[len(posts)-1].Uri})
	d.CreatePost(&TestPostRefInput{Actor: actor.Did, Reply: posts[len(posts)-1].Uri})
	posts = append(posts, d.CreatePost(&TestPostRefInput{Actor: actor.Did}))
	d.CreatePost(&TestPostRefInput{Actor: actor.Did, Reply: posts[len(posts)-1].Uri})
	d.CreatePost(&TestPostRefInput{Actor: actor.Did, Reply: posts[len(posts)-1].Uri})
	posts = append(posts, d.CreatePost(&TestPostRefInput{Actor: actor.Did}))
	d.CreatePost(&TestPostRefInput{Actor: actor.Did, Reply: posts[len(posts)-1].Uri})
	d.CreatePost(&TestPostRefInput{Actor: actor.Did, Reply: posts[len(posts)-1].Uri})
	posts = append(posts, d.CreatePost(&TestPostRefInput{Actor: actor.Did}))
	d.CreatePost(&TestPostRefInput{Actor: actor.Did, Reply: posts[len(posts)-1].Uri})
	d.CreatePost(&TestPostRefInput{Actor: actor.Did, Reply: posts[len(posts)-1].Uri})
	posts = append(posts, d.CreatePost(&TestPostRefInput{Actor: actor.Did}))
	d.CreatePost(&TestPostRefInput{Actor: actor.Did, Reply: posts[len(posts)-1].Uri})
	d.CreatePost(&TestPostRefInput{Actor: actor.Did, Reply: posts[len(posts)-1].Uri})

	slices.Reverse(posts)

	only, err := d.selectOnlyPosts([]int64{actor.ActorId}, SQLiteMaxInt, 2)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		CollectPostIds(posts[:2]),
		CollectPostIds(only),
	)

	only, err = d.selectOnlyPosts([]int64{actor.ActorId}, only[len(only)-1].PostId, 2)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		CollectPostIds(posts[2:4]),
		CollectPostIds(only),
	)

	only, err = d.selectOnlyPosts([]int64{actor.ActorId}, only[len(only)-1].PostId, 2)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		CollectPostIds(posts[4:6]),
		CollectPostIds(only),
	)

}
