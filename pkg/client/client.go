package client

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	cliutil "github.com/bluesky-social/indigo/util/cliutil"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/flicknow/go-bluesky-bot/pkg/clock"
	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/utils"
	cli "github.com/urfave/cli/v2"
)

type getPostsOutput struct {
	Posts []appbsky.FeedDefs_PostView `json:"posts" cborgen:"posts"`
}

type Client interface {
	Did() string
	Refresh() error
	AppendList(list string, actor string) (*comatproto.RepoCreateRecord_Output, error)
	GetActor(did string) (*appbsky.ActorDefs_ProfileViewDetailed, error)
	GetActors(dids []string) ([]*appbsky.ActorDefs_ProfileViewDetailed, error)
	GetAuthorFeed(handle string, filter string, limit int64, cursor string) ([]*appbsky.FeedDefs_FeedViewPost, string, error)
	GetFollows(did string, limit int64, cursor string) ([]*comatproto.RepoListRecords_Record, string, error)
	GetList(list string, cursor string, limit int64) ([]*appbsky.GraphDefs_ListItemView, string, error)
	GetLists(actor string, cursor string, limit int64) ([]*appbsky.GraphDefs_ListView, string, error)
	GetPosts(uris []string) ([]appbsky.FeedDefs_PostView, error)
	GetRecord(uri string, cid string) (*comatproto.RepoGetRecord_Output, error)
	Like(ref *comatproto.RepoStrongRef) (*comatproto.RepoCreateRecord_Output, error)
	Reply(text string, ref *comatproto.RepoStrongRef) (*comatproto.RepoCreateRecord_Output, error)
}
type defaultClient struct {
	CCtx      *cli.Context
	clock     clock.Clock
	debug     bool
	dryrun    bool
	reqmu     sync.Mutex
	refreshmu sync.Mutex
	xrpcc     *xrpc.Client
	did       string
}

func NewClient(cctx *cli.Context) (*defaultClient, error) {
	client := &defaultClient{
		CCtx:      cctx,
		clock:     clock.NewClock(),
		debug:     cmd.DebuggingEnabled(cctx, "client"),
		dryrun:    cctx.Bool("dry-run"),
		reqmu:     sync.Mutex{},
		refreshmu: sync.Mutex{},
	}

	err := client.setupAuthFile(false)
	if err != nil {
		return nil, err
	}

	xrpcc, err := cliutil.GetXrpcClient(cctx, true)
	if err != nil {
		return nil, err
	}
	xrpcc.Client.Transport = &xrpcInterceptorTransport{debug: client.debug}

	client.xrpcc = xrpcc
	client.did = xrpcc.Auth.Did

	return client, nil
}

func (c *defaultClient) Did() string {
	return c.xrpcc.Auth.Did
}

func (c *defaultClient) Refresh() error {
	c.refreshmu.Lock()
	defer c.refreshmu.Unlock()

	xrpcc, err := c.refreshClient()
	if err != nil {
		log.Printf("refresh err: %+v\n", err)
		return err
	}

	c.xrpcc = xrpcc

	return nil
}

type xrpcInterceptor struct {
	req *http.Request
	res *http.Response
}

type xrpcInterceptorTransport struct {
	debug bool
}

func (s *xrpcInterceptorTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	ctx := r.Context()
	intrcptr, ok := ctx.Value("xrpcInterceptor").(*xrpcInterceptor)

	res, err := http.DefaultTransport.RoundTrip(r)
	if ok {
		intrcptr.req = r
		intrcptr.res = res

	}

	if s.debug {
		fmt.Println(dumpHttp(r, res))
	}

	return res, err
}

func dumpInterceptor(ctx context.Context) string {
	intrcptr, ok := ctx.Value("xrpcInterceptor").(*xrpcInterceptor)
	if !ok {
		return ""
	}

	if intrcptr == nil {
		return ""
	}

	return dumpHttp(intrcptr.req, intrcptr.res)
}

func dumpHttp(req *http.Request, res *http.Response) string {
	bytes := []byte{}
	if req != nil {
		bytes, _ = httputil.DumpRequestOut(req, false)
	}

	if res != nil {
		respBytes, _ := httputil.DumpResponse(res, true)
		bytes = append(bytes, respBytes...)
	}

	if len(bytes) == 0 {
		return ""
	}

	return fmt.Sprintf("%s\n", bytes)
}

type loggingTransport struct{}

func (s *loggingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	bytes, _ := httputil.DumpRequestOut(r, true)

	resp, err := http.DefaultTransport.RoundTrip(r)
	// err is returned after dumping the response

	respBytes, _ := httputil.DumpResponse(resp, true)
	bytes = append(bytes, respBytes...)

	fmt.Printf("%s\n", bytes)

	return resp, err
}

func (c *defaultClient) intrcptrContext() context.Context {
	intrcptr := &xrpcInterceptor{}
	return context.WithValue(c.CCtx.Context, "xrpcInterceptor", intrcptr)
}

func intrcptrRes(ctx context.Context) *http.Response {
	intrcptr, ok := ctx.Value("xrpcInterceptor").(*xrpcInterceptor)
	if !ok {
		return nil
	}
	return intrcptr.res
}

func (c *defaultClient) GetActor(did string) (*appbsky.ActorDefs_ProfileViewDetailed, error) {
	if did == "" {
		return nil, fmt.Errorf("did cannot be an empty string")
	}

	c.reqmu.Lock()
	defer c.reqmu.Unlock()

	ctx := c.intrcptrContext()
	actor, err := appbsky.ActorGetProfile(ctx, c.xrpcc, did)
	if err != nil {
		c.Refresh()
		actor, err = appbsky.ActorGetProfile(ctx, c.xrpcc, did)
	}
	if (err != nil) && strings.Contains(err.Error(), "Profile not found") {
		return nil, nil
	} else if err != nil {
		fmt.Print(dumpInterceptor(ctx))
		return nil, err
	}
	return actor, nil
}

func (c *defaultClient) GetActors(dids []string) ([]*appbsky.ActorDefs_ProfileViewDetailed, error) {
	if (dids == nil) || (len(dids) == 0) {
		return nil, fmt.Errorf("dids cannot be empty")
	}

	c.reqmu.Lock()
	defer c.reqmu.Unlock()

	input := make(map[string]interface{})
	actors := appbsky.ActorGetProfiles_Output{}
	params := url.Values{}
	for _, did := range dids {
		params.Add("actors", did)
	}
	method := fmt.Sprintf("%s?%s", "app.bsky.actor.getProfiles", params.Encode())

	ctx := c.intrcptrContext()
	err := c.xrpcc.Do(ctx, xrpc.Query, "", method, input, nil, &actors)
	if err != nil {
		c.Refresh()
		err = c.xrpcc.Do(ctx, xrpc.Query, "", method, input, nil, &actors)
	}
	if err != nil {
		fmt.Print(dumpInterceptor(ctx))
		return nil, err
	}

	return actors.Profiles, nil
}

func (c *defaultClient) GetAuthorFeed(handle string, filter string, limit int64, cursor string) ([]*appbsky.FeedDefs_FeedViewPost, string, error) {
	if handle == "" {
		return nil, "", fmt.Errorf("did cannot be empty")
	}

	c.reqmu.Lock()
	defer c.reqmu.Unlock()

	ctx := c.intrcptrContext()
	feed, err := appbsky.FeedGetAuthorFeed(ctx, c.xrpcc, handle, cursor, filter, false, limit)
	if err != nil {
		c.Refresh()
		feed, err = appbsky.FeedGetAuthorFeed(ctx, c.xrpcc, handle, cursor, filter, false, limit)
	}
	if err != nil {
		res := intrcptrRes(ctx)
		if res != nil {
			fmt.Print(dumpInterceptor(ctx))
		}
		return nil, "", err
	}

	cursor = ""
	if feed.Cursor != nil {
		cursor = *feed.Cursor
	}
	return feed.Feed, cursor, nil
}

func (c *defaultClient) GetFollows(did string, limit int64, cursor string) ([]*comatproto.RepoListRecords_Record, string, error) {
	if did == "" {
		return nil, "", fmt.Errorf("did cannot be empty")
	}

	c.reqmu.Lock()
	defer c.reqmu.Unlock()

	ctx := c.intrcptrContext()

	records, err := comatproto.RepoListRecords(ctx, c.xrpcc, "app.bsky.graph.follow", cursor, limit, did, false, "", "")
	if err != nil {
		c.Refresh()
		records, err = comatproto.RepoListRecords(ctx, c.xrpcc, "app.bsky.graph.follow", cursor, limit, did, false, "", "")
	}
	if err != nil {
		res := intrcptrRes(ctx)
		if res != nil {
			fmt.Print(dumpInterceptor(ctx))
		}
		return nil, "", err
	}

	cursor = ""
	if records.Cursor != nil {
		cursor = *records.Cursor
	}

	return records.Records, cursor, nil
}

func (c *defaultClient) AppendList(list string, actor string) (*comatproto.RepoCreateRecord_Output, error) {
	if c.dryrun {
		log.Printf("DRY-RUN: would have added %s to %s", actor, list)
	}

	return c.request(&comatproto.RepoCreateRecord_Input{
		Collection: "app.bsky.graph.listitem",
		Repo:       c.xrpcc.Auth.Did,
		Record: &lexutil.LexiconTypeDecoder{
			Val: &appbsky.GraphListitem{
				CreatedAt: nowString(),
				List:      list,
				Subject:   actor,
			},
		},
	})
}

func (c *defaultClient) GetList(list string, cursor string, limit int64) ([]*appbsky.GraphDefs_ListItemView, string, error) {
	c.reqmu.Lock()
	defer c.reqmu.Unlock()

	ctx := c.intrcptrContext()
	output, err := appbsky.GraphGetList(ctx, c.xrpcc, cursor, limit, list)
	if err != nil {
		c.Refresh()
		output, err = appbsky.GraphGetList(ctx, c.xrpcc, cursor, limit, list)
	}
	if err != nil {
		res := intrcptrRes(ctx)
		if res != nil {
			fmt.Print(dumpInterceptor(ctx))
		}
		return nil, "", err
	}

	cursor = ""
	if output.Cursor != nil {
		cursor = *output.Cursor
	}
	return output.Items, cursor, nil
}

func (c *defaultClient) GetLists(actor string, cursor string, limit int64) ([]*appbsky.GraphDefs_ListView, string, error) {
	c.reqmu.Lock()
	defer c.reqmu.Unlock()

	ctx := c.intrcptrContext()
	output, err := appbsky.GraphGetLists(ctx, c.xrpcc, actor, cursor, limit)
	if err != nil {
		c.Refresh()
		output, err = appbsky.GraphGetLists(ctx, c.xrpcc, actor, cursor, limit)
	}
	if err != nil {
		res := intrcptrRes(ctx)
		if res != nil {
			fmt.Print(dumpInterceptor(ctx))
		}
		return nil, "", err
	}

	cursor = ""
	if output.Cursor != nil {
		cursor = *output.Cursor
	}
	return output.Lists, cursor, nil
}

func (c *defaultClient) GetPosts(uris []string) ([]appbsky.FeedDefs_PostView, error) {
	if (uris == nil) || (len(uris) == 0) {
		return nil, fmt.Errorf("uris cannot be empty")
	}

	c.reqmu.Lock()
	defer c.reqmu.Unlock()

	output := getPostsOutput{}
	input := make(map[string]interface{})

	params := url.Values{}
	for _, uri := range uris {
		params.Add("uris", uri)
	}
	method := fmt.Sprintf("%s?%s", "app.bsky.feed.getPosts", params.Encode())

	ctx := c.intrcptrContext()
	err := c.xrpcc.Do(ctx, xrpc.Query, "", method, input, nil, &output)
	if err != nil {
		c.Refresh()
		err = c.xrpcc.Do(ctx, xrpc.Query, "", method, input, nil, &output)
	}
	if err != nil {
		res := intrcptrRes(ctx)
		if res != nil {
			fmt.Print(dumpInterceptor(ctx))
		}
		return nil, err
	}

	return output.Posts, nil
}

func (c *defaultClient) GetRecord(uri string, cid string) (*comatproto.RepoGetRecord_Output, error) {
	parts := strings.Split(uri[5:], "/")
	repo := parts[0]
	collection := parts[1]
	rkey := parts[2]

	c.reqmu.Lock()
	defer c.reqmu.Unlock()

	ctx := c.intrcptrContext()

	record, err := comatproto.RepoGetRecord(ctx, c.xrpcc, cid, collection, repo, rkey)
	if err != nil {
		c.Refresh()
		record, err = comatproto.RepoGetRecord(ctx, c.xrpcc, cid, collection, repo, rkey)
	}

	if err != nil {
		res := intrcptrRes(ctx)
		if res != nil {
			fmt.Print(dumpInterceptor(ctx))
		}
		return nil, err
	}

	return record, nil
}

func (c *defaultClient) Like(ref *comatproto.RepoStrongRef) (*comatproto.RepoCreateRecord_Output, error) {
	if c.dryrun {
		log.Printf("DRY-RUN: would have liked %s", ref.Uri)
	}

	return c.request(&comatproto.RepoCreateRecord_Input{
		Collection: "app.bsky.feed.like",
		Repo:       c.xrpcc.Auth.Did,
		Record: &lexutil.LexiconTypeDecoder{
			Val: &appbsky.FeedLike{
				CreatedAt: c.clock.NowString(),
				Subject:   ref,
			}},
	})
}

func (c *defaultClient) Reply(text string, ref *comatproto.RepoStrongRef) (*comatproto.RepoCreateRecord_Output, error) {
	if c.dryrun {
		log.Printf("DRY-RUN: would have replied to %s", ref.Uri)
	}

	return c.request(&comatproto.RepoCreateRecord_Input{
		Collection: "app.bsky.feed.post",
		Repo:       c.xrpcc.Auth.Did,
		Record: &lexutil.LexiconTypeDecoder{Val: &appbsky.FeedPost{
			Text:      text,
			CreatedAt: c.clock.NowString(),
			Reply: &appbsky.FeedPost_ReplyRef{
				Parent: ref,
				Root:   ref,
			},
		}},
	})
}

func (c *defaultClient) request(input *comatproto.RepoCreateRecord_Input) (*comatproto.RepoCreateRecord_Output, error) {
	c.reqmu.Lock()
	defer c.reqmu.Unlock()

	if c.dryrun {
		return &comatproto.RepoCreateRecord_Output{}, nil
	}

	ctx := c.intrcptrContext()
	output, err := comatproto.RepoCreateRecord(ctx, c.xrpcc, input)
	if err == nil {
		return output, nil
	}

	c.Refresh()

	output, err = comatproto.RepoCreateRecord(ctx, c.xrpcc, input)
	if err != nil {
		res := intrcptrRes(ctx)
		if res != nil {
			fmt.Print(dumpInterceptor(ctx))
		}
		return output, err
	}

	return output, nil
}

func nowString() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func nowUnix() int64 {
	return time.Now().UTC().Unix()
}

func (c *defaultClient) setupAuthFile(reset bool) error {
	cctx := c.CCtx
	filename := cctx.String("auth")

	_, err := os.Stat(filename)
	if !reset && (err == nil) {
		return nil
	}

	xrpcc, err := cliutil.GetXrpcClient(cctx, false)
	if err != nil {
		return err
	}
	xrpcc.Client.Transport = &xrpcInterceptorTransport{debug: c.debug}

	ctx := c.intrcptrContext()
	handle := cctx.String("username")
	password := cctx.String("password")
	ses, err := comatproto.ServerCreateSession(ctx, xrpcc, &comatproto.ServerCreateSession_Input{
		Identifier: handle,
		Password:   password,
	})
	if err != nil {
		res := intrcptrRes(ctx)
		if res != nil {
			fmt.Print(dumpInterceptor(ctx))
		}
		return err
	}

	data, err := json.MarshalIndent(ses, "", "  ")
	if err != nil {
		return err
	}

	err = utils.WriteFile(filename, data)
	if err != nil {
		return err
	}

	return nil
}

func (c *defaultClient) refreshClient() (*xrpc.Client, error) {
	cctx := c.CCtx
	filename := cctx.String("auth")

	_, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}

	xrpcc, err := cliutil.GetXrpcClient(cctx, true)
	if err != nil {
		return nil, err
	}
	xrpcc.Client.Transport = &xrpcInterceptorTransport{debug: c.debug}

	a := xrpcc.Auth
	a.AccessJwt = a.RefreshJwt

	ctx := c.intrcptrContext()
	nauth, err := comatproto.ServerRefreshSession(ctx, xrpcc)
	if err != nil {
		res := intrcptrRes(ctx)
		if res != nil {
			fmt.Print(dumpInterceptor(ctx))
		}

		panic(err)
	}

	data, err := json.MarshalIndent(nauth, "", "  ")
	if err != nil {
		return nil, err
	}

	err = utils.WriteFile(filename, data)
	if err != nil {
		return nil, err
	}

	return cliutil.GetXrpcClient(cctx, true)
}
