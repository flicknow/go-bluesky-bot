package indexer

import (
	"context"
	"sync"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/lex/util"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/flicknow/go-bluesky-bot/pkg/client"
	"github.com/flicknow/go-bluesky-bot/pkg/clock"
	"github.com/flicknow/go-bluesky-bot/pkg/dbx"
	"github.com/flicknow/go-bluesky-bot/pkg/ticker"
	"github.com/flicknow/go-bluesky-bot/pkg/utils"
	"github.com/stretchr/testify/assert"

	"testing"
)

func NewTestIndexer(ctx context.Context, db *dbx.DBx) *Indexer {
	clk, ok := ctx.Value("clock").(clock.Clock)
	if !ok {
		clk = clock.NewClock()
	}

	cli, ok := ctx.Value("client").(client.Client)
	if !ok {
		cli = client.NewMockClient(ctx)
	}

	indexer := &Indexer{
		Client:           cli,
		Db:               db,
		clock:            clk,
		extendedIndexing: true,
		labelTicker:      ticker.NewTicker(0),
		wg:               &sync.WaitGroup{},
	}
	return indexer

}
func TestInsertNewskie(t *testing.T) {
	d, cleanup := dbx.NewTestDBx()
	defer cleanup()

	i := NewTestIndexer(context.Background(), d.DBx)

	elder := d.CreateActor()
	elder.CreatedAt = i.clock.NowUnix()
	d.InitActorInfo(elder, []*dbx.PostLabelRow{})
	old := d.CreatePost(&dbx.TestPostRefInput{Actor: elder.Did})

	newskie := d.CreateActor()
	newskie.CreatedAt = i.clock.NowUnix()
	d.InitActorInfo(newskie, []*dbx.PostLabelRow{})

	reply := dbx.NewTestPostRef(&dbx.TestPostRefInput{Actor: newskie.Did, Reply: old.Uri})

	_, err := i.Post(reply)
	if err != nil {
		panic(err)
	}

	newskies, err := d.SelectPostsByLabels(dbx.SQLiteMaxInt, 10, "newskie")
	if err != nil {
		panic(err)
	}

	assert.Equal(
		t,
		[]*dbx.PostRow{},
		newskies,
	)

	post := dbx.NewTestPostRef(&dbx.TestPostRefInput{Actor: newskie.Did})
	_, err = i.Post(post)
	if err != nil {
		panic(err)
	}

	newskies, err = d.SelectPostsByLabels(dbx.SQLiteMaxInt-1, 10, "newskie")
	if err != nil {
		panic(err)
	}

	assert.Equal(
		t,
		[]string{post.Ref.Uri},
		dbx.CollectUris(newskies),
	)
}

func TestInsertNewskieOnInit(t *testing.T) {
	cli := client.NewMockClient(context.Background())
	ctx := context.WithValue(context.Background(), "client", cli)

	d, cleanup := dbx.NewTestDBxContext(ctx)
	defer cleanup()

	actor := d.CreateActor()
	i := NewTestIndexer(ctx, d.DBx)

	postRef := dbx.NewTestPostRef(&dbx.TestPostRefInput{Actor: actor.Did})
	post, err := d.InsertPost(postRef, actor)
	if err != nil {
		panic(err)
	}

	newskies, err := d.SelectPostsByLabels(dbx.SQLiteMaxInt, 10, "newskie")
	if err != nil {
		panic(err)
	}

	assert.Equal(
		t,
		[]*dbx.PostRow{},
		newskies,
	)

	cli.MockGetActors = func(dids []string) ([]*bsky.ActorDefs_ProfileViewDetailed, error) {
		var postCount int64 = 1
		return []*bsky.ActorDefs_ProfileViewDetailed{
			{
				Did:        actor.Did,
				PostsCount: &postCount,
			},
		}, nil
	}
	cli.MockGetAuthorFeed = func(handle, filter string, limit int64, cursor string) ([]*bsky.FeedDefs_FeedViewPost, string, error) {
		return []*bsky.FeedDefs_FeedViewPost{
			{
				Post: &bsky.FeedDefs_PostView{
					Author: &bsky.ActorDefs_ProfileViewBasic{
						Did: actor.Did,
					},
					IndexedAt: i.clock.NowString(),
					Record:    &util.LexiconTypeDecoder{Val: postRef.Post},
					Uri:       post.Uri,
				},
			},
		}, "", nil
	}

	_, _, err = i.InitUninitializedActors(100, 0, 10)
	if err != nil {
		panic(err)
	}

	newskies, err = d.SelectPostsByLabels(dbx.SQLiteMaxInt-1, 10, "newskie")
	if err != nil {
		panic(err)
	}

	assert.Equal(
		t,
		[]string{post.Uri},
		dbx.CollectUris(newskies),
	)
}

func TestInitActorInfoSetsPostCount(t *testing.T) {
	cli := client.NewMockClient(context.Background())
	ctx := context.WithValue(context.Background(), "client", cli)

	d, cleanup := dbx.NewTestDBxContext(ctx)
	defer cleanup()

	actor := d.CreateActor()
	i := NewTestIndexer(ctx, d.DBx)

	postRef := dbx.NewTestPostRef(&dbx.TestPostRefInput{Actor: actor.Did})
	post, err := d.InsertPost(postRef, actor)
	if err != nil {
		panic(err)
	}

	cli.MockGetActors = func(dids []string) ([]*bsky.ActorDefs_ProfileViewDetailed, error) {
		var postCount int64 = 10
		return []*bsky.ActorDefs_ProfileViewDetailed{
			{
				Did:        actor.Did,
				PostsCount: &postCount,
			},
		}, nil
	}
	cli.MockGetAuthorFeed = func(handle, filter string, limit int64, cursor string) ([]*bsky.FeedDefs_FeedViewPost, string, error) {
		return []*bsky.FeedDefs_FeedViewPost{
			{
				Post: &bsky.FeedDefs_PostView{
					Author: &bsky.ActorDefs_ProfileViewBasic{
						Did: actor.Did,
					},
					IndexedAt: i.clock.NowString(),
					Record:    &util.LexiconTypeDecoder{Val: postRef.Post},
					Uri:       post.Uri,
				},
			},
		}, "", nil
	}

	_, _, err = i.InitUninitializedActors(100, 0, 10)
	if err != nil {
		panic(err)
	}

	actor, err = d.Actors.FindActor(actor.Did)
	if err != nil {
		panic(err)
	}

	assert.Equal(
		t,
		int64(1),
		actor.Posts,
	)
}

func TestInitActorInfoSetsPostCountWhenNoPosts(t *testing.T) {
	cli := client.NewMockClient(context.Background())
	ctx := context.WithValue(context.Background(), "client", cli)

	d, cleanup := dbx.NewTestDBxContext(ctx)
	defer cleanup()

	actor := d.CreateActor()
	i := NewTestIndexer(ctx, d.DBx)

	cli.MockGetActors = func(dids []string) ([]*bsky.ActorDefs_ProfileViewDetailed, error) {
		var postCount int64 = 1
		return []*bsky.ActorDefs_ProfileViewDetailed{
			{
				Did:        actor.Did,
				PostsCount: &postCount,
			},
		}, nil
	}

	_, _, err := i.InitUninitializedActors(100, 0, 10)
	if err != nil {
		panic(err)
	}

	actor, err = d.Actors.FindActor(actor.Did)
	if err != nil {
		panic(err)
	}

	assert.Equal(
		t,
		int64(0),
		actor.Posts,
	)
}

func TestLabelerRateLimitForActors(t *testing.T) {
	cli := client.NewMockClient(context.Background())
	clk := clock.NewMockClock()
	ctx := context.WithValue(context.Background(), "client", cli)
	ctx = context.WithValue(ctx, "clock", clk)

	d, cleanup := dbx.NewTestDBxContext(ctx)
	defer cleanup()

	i := NewTestIndexer(ctx, d.DBx)
	assert.NotNil(t, i)

	for i := 1; i <= 100; i++ {
		d.CreateActor()
	}

	d.CreatePost(&dbx.TestPostRefInput{Actor: d.CreateActor().Did})
	d.Posts.MustExec("UPDATE posts SET labeled = 0")

	getActorsHits := 0
	cli.MockGetActors = func(dids []string) ([]*bsky.ActorDefs_ProfileViewDetailed, error) {
		getActorsHits++
		var zero int64 = 0
		profiles := make([]*bsky.ActorDefs_ProfileViewDetailed, 0)
		for _, did := range dids {
			profiles = append(profiles, &bsky.ActorDefs_ProfileViewDetailed{
				Did:        did,
				PostsCount: &zero,
			})
		}
		return profiles, nil
	}

	clk.SetNow(clk.NowUnix() + 600)

	i.runLabelerOnce(5)
	assert.Equal(
		t,
		2,
		getActorsHits,
	)
}

func TestLabelerRateLimitForFollows(t *testing.T) {
	cli := client.NewMockClient(context.Background())
	clk := clock.NewMockClock()
	ctx := context.WithValue(context.Background(), "client", cli)
	ctx = context.WithValue(ctx, "clock", clk)

	d, cleanup := dbx.NewTestDBxContext(ctx)
	defer cleanup()

	i := NewTestIndexer(ctx, d.DBx)
	assert.NotNil(t, i)

	for i := 1; i <= 100; i++ {
		actor := d.CreateActor()
		_, err := d.FollowsIndexed.FindOrCreateByActorId(actor.ActorId)
		if err != nil {
			panic(err)
		}
	}

	getFollowsHits := 0
	cli.MockGetActors = func(dids []string) ([]*bsky.ActorDefs_ProfileViewDetailed, error) {
		var zero int64 = 0
		profiles := make([]*bsky.ActorDefs_ProfileViewDetailed, 0)
		for _, did := range dids {
			profiles = append(profiles, &bsky.ActorDefs_ProfileViewDetailed{
				Did:        did,
				PostsCount: &zero,
			})
		}
		return profiles, nil
	}
	cli.MockGetFollows = func(did string, limit int64, cursor string) ([]*atproto.RepoListRecords_Record, string, error) {
		getFollowsHits++
		return []*atproto.RepoListRecords_Record{}, "", nil
	}

	clk.SetNow(clk.NowUnix() + 600)

	i.runLabelerOnce(5)
	assert.Equal(
		t,
		3,
		getFollowsHits,
	)
}

func TestIndexNewskieFollows(t *testing.T) {
	d, cleanup := dbx.NewTestDBx()
	defer cleanup()

	i := NewTestIndexer(context.Background(), d.DBx)
	did := utils.NewTestDid()

	err := i.Newskie(did)
	if err != nil {
		panic(err)
	}

	newksie, err := d.Actors.FindOrCreateActor(did)
	if err != nil {
		panic(err)
	}

	index, err := d.FollowsIndexed.FindByActorId(newksie.ActorId)
	if err != nil {
		panic(err)
	}

	if assert.NotNil(t, index) {
		assert.Equal(t, int64(0), index.LastFollow)
	}
}

func TestLabelerResumeFollowsIndexingWithCursor(t *testing.T) {
	cli := client.NewMockClient(context.Background())
	ctx := context.WithValue(context.Background(), "client", cli)

	d, cleanup := dbx.NewTestDBxContext(ctx)
	defer cleanup()

	i := NewTestIndexer(ctx, d.DBx)
	assert.NotNil(t, i)

	actor := d.CreateActor()
	follows := []*dbx.ActorRow{
		d.CreateActor(),
		d.CreateActor(),
	}

	_, err := d.FollowsIndexed.FindOrCreateByActorId(actor.ActorId)
	if err != nil {
		panic(err)
	}

	cli.MockGetFollows = func(did string, limit int64, cursor string) ([]*atproto.RepoListRecords_Record, string, error) {
		record := &atproto.RepoListRecords_Record{}
		record.Uri = dbx.NewTestUri("app.bsky.graph.follow", actor.Did)

		follow := &bsky.GraphFollow{LexiconTypeID: "app.bsky.graph.follow"}
		record.Value = &lexutil.LexiconTypeDecoder{Val: follow}

		if cursor == follows[1].Did {
			follow.Subject = follows[1].Did
			return []*atproto.RepoListRecords_Record{record}, "", nil
		} else {
			follow.Subject = follows[0].Did
			return []*atproto.RepoListRecords_Record{record}, follows[1].Did, nil
		}
	}

	count, err := i.runLabelerOnceForFollows(1)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, int64(1), count)

	indexed, err := d.FollowsIndexed.FindOrCreateByActorId(actor.ActorId)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, follows[1].Did, indexed.Cursor)
	assert.Equal(t, int64(-1), indexed.LastFollow)

	count, err = i.runLabelerOnceForFollows(1)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, int64(1), count)

	indexed, err = d.FollowsIndexed.FindOrCreateByActorId(actor.ActorId)
	if err != nil {
		panic(err)
	}
	assert.NotEqual(t, int64(-1), indexed.LastFollow)
}

func TestInitNewskieActorInfo(t *testing.T) {
	d, cleanup := dbx.NewTestDBx()
	defer cleanup()

	i := NewTestIndexer(context.Background(), d.DBx)
	did := utils.NewTestDid()

	err := i.Newskie(did)
	if err != nil {
		panic(err)
	}

	newksie, err := d.Actors.FindOrCreateActor(did)
	if err != nil {
		panic(err)
	}

	assert.Equal(t, i.clock.NowUnix(), newksie.CreatedAt)
}
