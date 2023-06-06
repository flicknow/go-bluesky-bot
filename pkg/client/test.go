package client

import (
	"context"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/flicknow/go-bluesky-bot/pkg/utils"
)

type mockClient struct {
	did         string
	MockRefresh func() error

	MockAppendList    func(list string, actor string) (*comatproto.RepoCreateRecord_Output, error)
	MockGetActor      func(did string) (*appbsky.ActorDefs_ProfileViewDetailed, error)
	MockGetActors     func(dids []string) ([]*appbsky.ActorDefs_ProfileViewDetailed, error)
	MockGetAuthorFeed func(handle string, filter string, limit int64, cursor string) ([]*appbsky.FeedDefs_FeedViewPost, string, error)
	MockGetFollows    func(did string, limit int64, cursor string) ([]*comatproto.RepoListRecords_Record, string, error)
	MockGetList       func(list string, cursor string, limit int64) ([]*appbsky.GraphDefs_ListItemView, string, error)
	MockGetLists      func(actor string, cursor string, limit int64) ([]*appbsky.GraphDefs_ListView, string, error)
	MockGetPosts      func(uris []string) ([]appbsky.FeedDefs_PostView, error)
	MockGetRecord     func(uri string, cid string) (*comatproto.RepoGetRecord_Output, error)
	MockLike          func(ref *comatproto.RepoStrongRef) (*comatproto.RepoCreateRecord_Output, error)
	MockReply         func(text string, ref *comatproto.RepoStrongRef) (*comatproto.RepoCreateRecord_Output, error)
}

func (c *mockClient) defaultRefresh() error {
	return nil
}
func (c *mockClient) defaultAppendList(list string, actor string) (*comatproto.RepoCreateRecord_Output, error) {
	return &comatproto.RepoCreateRecord_Output{}, nil
}
func (c *mockClient) defaultGetActor(did string) (*appbsky.ActorDefs_ProfileViewDetailed, error) {
	return nil, nil
}
func (c *mockClient) defaultGetActors(dids []string) ([]*appbsky.ActorDefs_ProfileViewDetailed, error) {
	return []*appbsky.ActorDefs_ProfileViewDetailed{}, nil
}
func (c *mockClient) defaultGetAuthorFeed(handle string, filter string, limit int64, cursor string) ([]*appbsky.FeedDefs_FeedViewPost, string, error) {
	return []*appbsky.FeedDefs_FeedViewPost{}, "", nil
}
func (c *mockClient) defaultGetFollows(did string, limit int64, cursor string) ([]*comatproto.RepoListRecords_Record, string, error) {
	return []*comatproto.RepoListRecords_Record{}, "", nil
}
func (c *mockClient) defaultGetList(list string, cursor string, limit int64) ([]*appbsky.GraphDefs_ListItemView, string, error) {
	return []*appbsky.GraphDefs_ListItemView{}, "", nil
}
func (c *mockClient) defaultGetLists(actor string, cursor string, limit int64) ([]*appbsky.GraphDefs_ListView, string, error) {
	return []*appbsky.GraphDefs_ListView{}, "", nil
}
func (c *mockClient) defaultGetPosts(uris []string) ([]appbsky.FeedDefs_PostView, error) {
	return []appbsky.FeedDefs_PostView{}, nil
}
func (c *mockClient) defaultGetRecord(uri string, cid string) (*comatproto.RepoGetRecord_Output, error) {
	return &comatproto.RepoGetRecord_Output{}, nil
}
func (c *mockClient) defaultLike(ref *comatproto.RepoStrongRef) (*comatproto.RepoCreateRecord_Output, error) {
	return &comatproto.RepoCreateRecord_Output{}, nil
}
func (c *mockClient) defaultReply(text string, ref *comatproto.RepoStrongRef) (*comatproto.RepoCreateRecord_Output, error) {
	return &comatproto.RepoCreateRecord_Output{}, nil
}

func (c *mockClient) Did() string {
	return c.did
}

func (c *mockClient) Refresh() error {
	return c.MockRefresh()
}
func (c *mockClient) AppendList(list string, actor string) (*comatproto.RepoCreateRecord_Output, error) {
	return c.MockAppendList(list, actor)
}
func (c *mockClient) GetActor(did string) (*appbsky.ActorDefs_ProfileViewDetailed, error) {
	return c.MockGetActor(did)
}
func (c *mockClient) GetActors(dids []string) ([]*appbsky.ActorDefs_ProfileViewDetailed, error) {
	return c.MockGetActors(dids)

}
func (c *mockClient) GetAuthorFeed(handle string, filter string, limit int64, cursor string) ([]*appbsky.FeedDefs_FeedViewPost, string, error) {
	return c.MockGetAuthorFeed(handle, filter, limit, cursor)
}
func (c *mockClient) GetFollows(did string, limit int64, cursor string) ([]*comatproto.RepoListRecords_Record, string, error) {
	return c.MockGetFollows(did, limit, cursor)
}
func (c *mockClient) GetList(list string, cursor string, limit int64) ([]*appbsky.GraphDefs_ListItemView, string, error) {
	return c.MockGetList(list, cursor, limit)
}
func (c *mockClient) GetLists(actor string, cursor string, limit int64) ([]*appbsky.GraphDefs_ListView, string, error) {
	return c.MockGetLists(actor, cursor, limit)
}
func (c *mockClient) GetPosts(uris []string) ([]appbsky.FeedDefs_PostView, error) {
	return c.MockGetPosts(uris)
}
func (c *mockClient) GetRecord(uri string, cid string) (*comatproto.RepoGetRecord_Output, error) {
	return c.MockGetRecord(uri, cid)
}
func (c *mockClient) Like(ref *comatproto.RepoStrongRef) (*comatproto.RepoCreateRecord_Output, error) {
	return c.MockLike(ref)
}
func (c *mockClient) Reply(text string, ref *comatproto.RepoStrongRef) (*comatproto.RepoCreateRecord_Output, error) {
	return c.MockReply(text, ref)
}

func NewMockClient(ctx context.Context) *mockClient {
	did, ok := ctx.Value("did").(string)
	if !ok {
		did = utils.NewTestDid()
	}

	c := &mockClient{did: did}
	c.MockRefresh = c.defaultRefresh
	c.MockAppendList = c.defaultAppendList
	c.MockGetActor = c.defaultGetActor
	c.MockGetActors = c.defaultGetActors
	c.MockGetAuthorFeed = c.defaultGetAuthorFeed
	c.MockGetFollows = c.defaultGetFollows
	c.MockGetList = c.defaultGetList
	c.MockGetLists = c.defaultGetLists
	c.MockGetPosts = c.defaultGetPosts
	c.MockGetRecord = c.defaultGetRecord
	c.MockLike = c.defaultLike
	c.MockReply = c.defaultReply

	return c
}
