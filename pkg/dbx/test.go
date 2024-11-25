package dbx

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/crypto"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/flicknow/go-bluesky-bot/pkg/firehose"
	"github.com/flicknow/go-bluesky-bot/pkg/utils"
)

func CollectPostIds(posts []*PostRow) []int64 {
	postids := make([]int64, len(posts))
	for i, post := range posts {
		postids[i] = post.PostId
	}
	return postids
}

func CollectUris(posts []*PostRow) []string {
	uris := make([]string, len(posts))
	for i, post := range posts {
		uris[i] = post.Uri
	}
	return uris
}

func SortInt64s(unsorted []int64) []int64 {
	sorted := make([]int64, len(unsorted))
	for i, val := range unsorted {
		sorted[i] = val
	}

	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	return sorted
}

func QueryPks(d queryable) []int64 {
	st := reflect.TypeOf(d)
	field, ok := st.Elem().FieldByName("DB")
	if !ok {
		panic(fmt.Sprintf("type %s does not have a DB field", field.Name))
	}

	tableName := field.Tag.Get("dbx-table")
	pk := field.Tag.Get("dbx-pk")

	rows, err := d.Query(fmt.Sprintf("SELECT %s FROM %s ORDER BY %s ASC", pk, tableName, pk))
	if err != nil {
		panic(err)
	}

	results := make([]int64, 0)
	for rows.Next() {
		var result int64 = 0
		err := rows.Scan(&result)
		if err != nil {
			panic(err)
		}
		results = append(results, result)
	}

	return results
}

func DumpDb(q queryable) string {
	tableSt := reflect.TypeOf(q)

	dbField, ok := tableSt.Elem().FieldByName("DB")
	if !ok {
		panic(fmt.Sprintf("type %s does not have a DB field", tableSt.Name()))
	}
	tableName := dbField.Tag.Get("dbx-table")
	pk := dbField.Tag.Get("dbx-pk")

	tableVal := reflect.ValueOf(q).Elem()
	pathField := tableVal.FieldByName("path")
	path := pathField.String()

	query := fmt.Sprintf("SELECT * FROM %s ORDER BY %s ASC", tableName, pk)
	bytes, err := exec.Command("sqlite3", "-json", path, query).Output()
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

func NewTestUri(lex string, did string) string {
	rkey := fmt.Sprintf("%d", rand.Int63())
	return fmt.Sprintf("at://%s/%s/%s", did, lex, rkey)
}

func NewTestPostUri(did string) string {
	return NewTestUri("app.bsky.feed.post", did)
}

type TestPostRefInput struct {
	Actor    string
	PostId   int64
	Mentions []string
	Quote    string
	Reply    string
	Text     string
	Uri      string
}

type TestPostInput struct {
	TestPostRefInput
	Labels []string
}

func NewTestPostRef(input *TestPostRefInput) *firehose.PostRef {
	ref := &atproto.RepoStrongRef{Uri: input.Uri}
	post := &bsky.FeedPost{CreatedAt: time.Now().UTC().Format(time.RFC3339), Reply: nil, Text: input.Text}
	if (input.Actor != "") && (input.Uri == "") {
		ref.Uri = NewTestPostUri(input.Actor)
	}
	if input.Reply != "" {
		post.Reply = &bsky.FeedPost_ReplyRef{
			Parent: &atproto.RepoStrongRef{Uri: input.Reply},
			Root:   &atproto.RepoStrongRef{Uri: input.Reply},
		}
	}

	mentions := []string{}
	if input.Mentions != nil {
		mentions = input.Mentions
	}

	return &firehose.PostRef{
		Post:     post,
		Ref:      ref,
		Mentions: mentions,
		Quotes:   input.Quote,
	}
}

type testDBx struct {
	*DBx
}

func NewTestDBxContext(ctx context.Context) (*testDBx, func()) {
	dir, err := os.MkdirTemp("", "*")
	if err != nil {
		panic(err)
	}

	priv, err := crypto.GeneratePrivateKeyK256()
	if err != nil {
		panic(err)
	}

	ctx = context.WithValue(ctx, "db-dir", dir)
	ctx = context.WithValue(ctx, "extended-indexing", true)
	ctx = context.WithValue(ctx, "signing-key", hex.EncodeToString(priv.Bytes()))

	return &testDBx{NewDBx(ctx)}, func() { os.RemoveAll(dir) }
}

func NewTestDBx() (*testDBx, func()) {
	return NewTestDBxContext(context.Background())
}

func (d *testDBx) CreateActor() *ActorRow {
	actorRow, err := d.Actors.FindOrCreateActor(utils.NewTestDid())
	if err != nil {
		panic(err)
	}

	return actorRow
}

func (d *testDBx) CreatePost(input *TestPostRefInput, labels ...string) *PostRow {
	postRef := NewTestPostRef(input)
	actor, err := d.Actors.FindOrCreateActor(utils.ParseDid(postRef.Ref.Uri))
	if err != nil {
		panic(err)
	}

	postRow, err := d.InsertPost(postRef, actor, labels...)
	if err != nil {
		panic(err)
	}

	return postRow
}

func (d *testDBx) CreateLike(actor *ActorRow, post *PostRow) *LikeRow {
	uri := fmt.Sprintf("at://%s/app.bsky.feed.like/%d", actor.Did, rand.Int63())
	err := d.InsertLike(&firehose.LikeRef{
		Like: &bsky.FeedLike{
			Subject: &comatproto.RepoStrongRef{Uri: post.Uri},
		},
		Ref: &comatproto.RepoStrongRef{Uri: uri},
	})
	if err != nil {
		panic(err)
	}

	like, err := d.Likes.FindByUri(uri)
	if err != nil {
		panic(err)
	}

	return like
}

func (d *testDBx) CreateRepost(actor *ActorRow, post *PostRow) *RepostRow {
	uri := fmt.Sprintf("at://%s/app.bsky.feed.repost/%d", actor.Did, rand.Int63())
	err := d.InsertRepost(&firehose.RepostRef{
		Repost: &bsky.FeedRepost{
			Subject: &comatproto.RepoStrongRef{Uri: post.Uri},
		},
		Ref: &comatproto.RepoStrongRef{Uri: uri},
	})
	if err != nil {
		panic(err)
	}

	repost, err := d.Reposts.FindByUri(uri)
	if err != nil {
		panic(err)
	}

	return repost
}
