package firehose

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/flicknow/go-bluesky-bot/pkg/client"
	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/utils"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/sequential"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/gorilla/websocket"
)

var DefaultBgsHost = "https://bsky.network"
var DmRegex = regexp.MustCompile(`(?i)^\W*DM\W*\s@\w+\.`)

type Firehose struct {
	Client       client.Client
	Cursor       int64
	addr         string
	con          *websocket.Conn
	conCtx       context.Context
	conCtxCancel context.CancelFunc
	mu           sync.Mutex
	cursorPath   string
	debug        bool

	errCh      chan error
	blockCh    chan *BlockRef
	deleteCh   chan *BareUri
	followCh   chan *FollowRef
	likeCh     chan *LikeRef
	newskiesCh chan *BareUri
	postCh     chan *PostRef
	repostCh   chan *RepostRef
}

type BareUri struct {
	Uri string
	Seq int64
}

type BlockRef struct {
	Subject string
	Ref     *comatproto.RepoStrongRef
	Seq     int64
}

type FollowRef struct {
	Subject string
	Ref     *comatproto.RepoStrongRef
	Seq     int64
}
type LikeRef struct {
	Like *appbsky.FeedLike
	Ref  *comatproto.RepoStrongRef
	Seq  int64
}

type PostRef struct {
	Post     *appbsky.FeedPost
	Ref      *comatproto.RepoStrongRef
	Mentions []string
	Quotes   string
	Seq      int64
}

type RepostRef struct {
	Repost *appbsky.FeedRepost
	Ref    *comatproto.RepoStrongRef
	Seq    int64
}

func NewPostRef(post *appbsky.FeedPost, ref *comatproto.RepoStrongRef, seq int64) *PostRef {
	postRef := &PostRef{
		Post:     post,
		Ref:      ref,
		Mentions: getMentions(post),
		Quotes:   getQuote(post),
		Seq:      seq,
	}

	return postRef
}

func (postRef *PostRef) HasMedia() bool {
	post := postRef.Post

	embed := post.Embed
	if embed != nil {
		if embed.EmbedExternal != nil {
			return true
		}
		if embed.EmbedImages != nil {
			return true
		}
		if embed.EmbedRecord != nil {
			return true
		}
		if embed.EmbedRecordWithMedia != nil {
			return true
		}
	}

	facets := post.Facets
	if facets == nil {
		return false
	}

	for _, facet := range facets {
		features := facet.Features
		if features == nil {
			continue
		}

		for _, feature := range features {
			// ignore mentions
			if feature.RichtextFacet_Mention == nil {
				return true
			}
		}
	}

	return false
}

func (postRef *PostRef) IsDm() bool {
	text := postRef.Post.Text
	if !DmRegex.MatchString(text) {
		return false
	}

	mentions := postRef.Mentions
	return (mentions != nil) && (len(mentions) > 0)
}

func getMentions(post *appbsky.FeedPost) []string {
	mentions := make([]string, 0)

	embed := post.Embed
	if (embed != nil) && (embed.EmbedRecord != nil) {
		record := embed.EmbedRecord.Record
		if record != nil {
			did := utils.ParseDid(record.Uri)
			if did != "" {
				mentions = append(mentions, did)
			}
		}
	}

	facets := post.Facets
	if facets == nil {
		return mentions
	}

	for _, facet := range facets {
		features := facet.Features
		if features == nil {
			continue
		}

		for _, feature := range features {
			if feature.RichtextFacet_Mention == nil {
				continue
			}
			if feature.RichtextFacet_Mention.Did != "" {
				mentions = append(mentions, feature.RichtextFacet_Mention.Did)
			}
		}
	}

	return mentions
}

func getQuote(post *appbsky.FeedPost) string {
	embed := post.Embed
	if embed == nil {
		return ""
	}

	if (embed.EmbedRecord != nil) && (embed.EmbedRecord.Record != nil) {
		return embed.EmbedRecord.Record.Uri
	}

	if (embed.EmbedRecordWithMedia != nil) && (embed.EmbedRecordWithMedia.Record != nil) && (embed.EmbedRecordWithMedia.Record.Record != nil) {
		return embed.EmbedRecordWithMedia.Record.Record.Uri
	}

	return ""
}

func (f *Firehose) Ack(seq int64) {
	if seq > f.Cursor {
		f.Cursor = seq

		if (seq % 1000) == 0 {
			f.saveCursor()
		}
	}

}

func NewFirehose(ctx context.Context, client client.Client) *Firehose {
	bgs, ok := ctx.Value("bgs-host").(string)
	if !ok {
		bgs = DefaultBgsHost
	}

	url, err := url.Parse(bgs)
	if err != nil {
		panic(err)
	}

	scheme := url.Scheme

	addr := ""
	if scheme == "http" {
		addr = "ws://"
	} else if scheme == "https" {
		addr = "wss://"
	} else {
		log.Panicf("unsupported bgs scheme %s for bgs %s", scheme, bgs)
	}
	addr = addr + url.Host + "/xrpc/com.atproto.sync.subscribeRepos"

	cursor, _ := ctx.Value("print-seq").(int64)
	if cursor > 1 {
		cursor--
	}

	cursorPath, _ := ctx.Value("cursor").(string)
	return &Firehose{
		addr:         addr,
		Cursor:       cursor,
		cursorPath:   cursorPath,
		Client:       client,
		con:          nil,
		conCtx:       nil,
		conCtxCancel: nil,
		debug:        cmd.DebuggingEnabled(ctx, "firehose"),
		mu:           sync.Mutex{},
		errCh:        nil,
		blockCh:      nil,
		deleteCh:     nil,
		followCh:     nil,
		likeCh:       nil,
		postCh:       nil,
		repostCh:     nil,
	}
}

func (f *Firehose) loadCursor() error {
	p := f.cursorPath
	if p == "" {
		return nil
	}

	b, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}

	cursor, err := strconv.ParseInt(strings.TrimSuffix(string(b), "\n"), 10, 64)
	if err != nil {
		return err
	}

	f.Cursor = cursor

	return nil
}

func (f *Firehose) saveCursor() error {
	p := f.cursorPath
	if p == "" {
		return nil
	}

	c := f.Cursor
	if c == 0 {
		return nil
	}

	return utils.WriteFile(p, []byte(fmt.Sprintf("%d", c)))
}

func (f *Firehose) Start(ctx context.Context) error {
	if (f.cursorPath != "") && (f.Cursor == 0) {
		err := f.loadCursor()
		if err != nil {
			return err
		}
	}

	if f.con == nil {
		conCtx, cancel := context.WithCancel(ctx)
		f.conCtx = conCtx
		f.conCtxCancel = cancel

		con, err := f.startStream()
		if err != nil {
			return err
		}
		f.con = con
	}
	return nil
}

func (f *Firehose) Restart(ctx context.Context) error {
	conCtxCancel := f.conCtxCancel
	if conCtxCancel != nil {
		conCtxCancel()
	}
	if f.con != nil {
		f.con.Close()
		f.con = nil
	}
	return f.Start(ctx)
}

func isHandledType(lextype string) bool {
	_, err := lexutil.NewFromType(lextype)
	return err == nil
}

func (f *Firehose) consumeStream(con *websocket.Conn) bool {
	ctx := f.conCtx
	if f.debug {
		fmt.Printf("> local conn=%s\n", con.LocalAddr().String())
	}

	first := true
	lastSeq := f.Cursor
	rsc := &events.RepoStreamCallbacks{
		RepoCommit: func(evt *comatproto.SyncSubscribeRepos_Commit) error {
			if first {
				if f.debug {
					fmt.Printf("saw first event: seq %d\n", evt.Seq)
				}
				first = false
				if lastSeq == 0 {
					lastSeq = evt.Seq
				}
			}
			if evt.TooBig {
				log.Printf("skipping too big events for now: %d\n", evt.Seq)
				return nil
			}
			if (evt.Seq - lastSeq) > 1000 {
				return fmt.Errorf("skipped too many seqs: went from %d to %d", lastSeq, evt.Seq)
			}
			lastSeq = evt.Seq

			r, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks))
			if err != nil {
				return fmt.Errorf("reading repo from car (seq: %d, len: %d): %w", evt.Seq, len(evt.Blocks), err)
			}
			for _, op := range evt.Ops {
				path := op.Path
				parts := strings.SplitN(path, "/", 2)
				lextype := parts[0]
				uri := fmt.Sprintf("at://%s/%s", evt.Repo, path)
				if !isHandledType(lextype) {
					continue
				}

				ek := repomgr.EventKind(op.Action)
				switch ek {
				case repomgr.EvtKindCreateRecord, repomgr.EvtKindUpdateRecord:
					rc, rec, err := r.GetRecord(ctx, op.Path)
					if err != nil {
						//e := fmt.Errorf("getting record %s (%s) within seq %d for %s: %w", op.Path, *op.Cid, evt.Seq, evt.Repo, err)
						//log.Printf("%+v\n", e)
						continue
					}
					if lexutil.LexLink(rc) != *op.Cid {
						return fmt.Errorf("mismatch in record and op cid: %s != %s", rc, *op.Cid)
					}
					ref := &comatproto.RepoStrongRef{Cid: rc.String(), Uri: uri}
					switch lextype {
					case "app.bsky.feed.like":
						like := &appbsky.FeedLike{}
						if r, ok := rec.(*appbsky.FeedLike); ok {
							like = r
						} else {
							err = utils.DecodeCBOR(rec, &like)
							if err != nil {
								log.Printf("error decoding %s: %+v", uri, err)
								return nil
							}
						}
						if f.likeCh != nil {
							f.likeCh <- &LikeRef{like, ref, evt.Seq}
						}
					case "app.bsky.feed.post":
						post := &appbsky.FeedPost{}
						if r, ok := rec.(*appbsky.FeedPost); ok {
							post = r
						} else {
							err = utils.DecodeCBOR(rec, &post)
							if err != nil {
								log.Printf("error decoding %s: %+v", uri, err)
								return nil
							}
						}
						if f.postCh != nil {
							f.postCh <- NewPostRef(post, ref, evt.Seq)
						}
					case "app.bsky.feed.repost":
						repost := &appbsky.FeedRepost{}
						if r, ok := rec.(*appbsky.FeedRepost); ok {
							repost = r
						} else {
							err = utils.DecodeCBOR(rec, &repost)
							if err != nil {
								log.Printf("error decoding %s: %+v", uri, err)
								return nil
							}
						}
						if f.repostCh != nil {
							f.repostCh <- &RepostRef{repost, ref, evt.Seq}
						}
					case "app.bsky.actor.profile":
						if (f.newskiesCh != nil) && (ek == repomgr.EvtKindCreateRecord) {
							f.newskiesCh <- &BareUri{evt.Repo, evt.Seq}
						}
					case "app.bsky.graph.block":
						block := &appbsky.GraphBlock{}
						if r, ok := rec.(*appbsky.GraphBlock); ok {
							block = r
						} else {
							err = utils.DecodeCBOR(rec, &block)
							if err != nil {
								log.Printf("error decoding %s: %+v", uri, err)
								return nil
							}
						}
						if (f.blockCh != nil) && (block.Subject == f.Client.Did()) {
							f.blockCh <- &BlockRef{block.Subject, ref, evt.Seq}
						}
					case "app.bsky.graph.follow":
						follow := &appbsky.GraphFollow{}
						if r, ok := rec.(*appbsky.GraphFollow); ok {
							follow = r
						} else {
							err = utils.DecodeCBOR(rec, &follow)
							if err != nil {
								log.Printf("error decoding %s: %+v", uri, err)
								return nil
							}
						}
						if f.followCh != nil {
							f.followCh <- &FollowRef{follow.Subject, ref, evt.Seq}
						}
					}
				case repomgr.EvtKindDeleteRecord:
					if f.deleteCh != nil {
						f.deleteCh <- &BareUri{uri, evt.Seq}
					}
				}
			}
			return nil
		},
		RepoHandle: func(evt *comatproto.SyncSubscribeRepos_Handle) error {
			if first {
				if f.debug {
					fmt.Printf("saw first event: seq %d\n", evt.Seq)
				}
				first = false
			}
			return nil
		},
		RepoInfo: func(evt *comatproto.SyncSubscribeRepos_Info) error {
			log.Printf("RepoInfo: %s\n", utils.Dump(evt))
			return nil
		},
		RepoMigrate: func(evt *comatproto.SyncSubscribeRepos_Migrate) error {
			if first {
				if f.debug {
					fmt.Printf("saw first event: seq %d\n", evt.Seq)
				}
				first = false
			}
			log.Printf("RepoMigrate: %s\n", utils.Dump(evt))
			return nil
		},
		RepoTombstone: func(evt *comatproto.SyncSubscribeRepos_Tombstone) error {
			if first {
				if f.debug {
					fmt.Printf("saw first event: seq %d\n", evt.Seq)
				}
				first = false
			}

			if f.blockCh != nil {
				uri := fmt.Sprintf("at://%s/tombstone/tombstone", evt.Did)
				ref := &comatproto.RepoStrongRef{Uri: uri}
				f.blockCh <- &BlockRef{f.Client.Did(), ref, evt.Seq}
			}

			return nil
		},
		LabelLabels: func(evt *comatproto.LabelSubscribeLabels_Labels) error {
			if first {
				if f.debug {
					fmt.Printf("saw first event: seq %d\n", evt.Seq)
				}
				first = false
			}
			log.Printf("LabelLabels: %s\n", utils.Dump(evt))
			return nil
		},
		LabelInfo: func(evt *comatproto.LabelSubscribeLabels_Info) error {
			log.Printf("LabelInfo: %s\n", utils.Dump(evt))
			return nil
		},
		Error: func(errf *events.ErrorFrame) error {
			err := fmt.Errorf("error frame: %s: %s", errf.Error, errf.Message)
			if f.errCh != nil {
				f.errCh <- err
			}
			return err
		},
	}

	seqScheduler := sequential.NewScheduler(con.RemoteAddr().String(), rsc.EventHandler)
	err := events.HandleRepoStream(ctx, con, seqScheduler)
	if (err != nil) && !strings.Contains(err.Error(), "use of closed network connection") {
		log.Printf("stream error %+v\n", err)

		if f.errCh != nil {
			f.errCh <- err
		}

		if errors.Is(err, context.Canceled) {
			seqScheduler.Shutdown()
			return false
		}
	}

	return true
}

func (f *Firehose) startStream() (*websocket.Conn, error) {
	addr := f.addr
	c := f.Cursor
	d := websocket.DefaultDialer
	if c != 0 {
		addr = fmt.Sprintf("%s?cursor=%d", addr, c)
	}

	con, _, err := d.Dial(addr, http.Header{})
	if err != nil {
		return nil, err
	}

	go func() {
		for f.con != nil {
			if !f.consumeStream(con) {
				return
			}
		}
	}()
	return con, nil
}

func (f *Firehose) Stop() {
	if f.conCtxCancel != nil {
		f.conCtxCancel()
	}
	f.CloseConnection()
	f.CloseChannels()
}

func (f *Firehose) CloseConnection() {
	if f.con != nil {
		con := f.con
		f.con = nil
		con.Close()
	}
	err := f.saveCursor()
	if err != nil {
		fmt.Printf("ERROR saving cursor %d to %s: %+v\n", f.Cursor, f.cursorPath, err)
	} else {
		fmt.Printf("SUCCESS saved cursor %d to %s\n", f.Cursor, f.cursorPath)
	}
}

func (f *Firehose) CloseChannels() {
	if f.errCh != nil {
		close(f.errCh)
		f.errCh = nil
	}
	if f.blockCh != nil {
		close(f.blockCh)
		f.blockCh = nil
	}
	if f.deleteCh != nil {
		close(f.deleteCh)
		f.deleteCh = nil
	}
	if f.followCh != nil {
		close(f.followCh)
		f.followCh = nil
	}
	if f.likeCh != nil {
		close(f.likeCh)
		f.likeCh = nil
	}
	if f.newskiesCh != nil {
		close(f.newskiesCh)
		f.newskiesCh = nil
	}
	if f.postCh != nil {
		close(f.postCh)
		f.postCh = nil
	}
	if f.repostCh != nil {
		close(f.repostCh)
		f.repostCh = nil
	}
}

func (f *Firehose) Errors() <-chan error {
	if f.errCh == nil {
		f.errCh = make(chan error, 1)
	}
	return f.errCh

}

func (f *Firehose) Blocks() <-chan *BlockRef {
	if f.blockCh == nil {
		f.blockCh = make(chan *BlockRef, 1)
	}
	return f.blockCh
}

func (f *Firehose) Deletes() <-chan *BareUri {
	if f.deleteCh == nil {
		f.deleteCh = make(chan *BareUri, 1)
	}
	return f.deleteCh
}

func (f *Firehose) Follows() <-chan *FollowRef {
	if f.followCh == nil {
		f.followCh = make(chan *FollowRef, 1)
	}
	return f.followCh
}

func (f *Firehose) Likes() <-chan *LikeRef {
	if f.likeCh == nil {
		f.likeCh = make(chan *LikeRef, 1)
	}
	return f.likeCh
}

func (f *Firehose) Newskies() <-chan *BareUri {
	if f.newskiesCh == nil {
		f.newskiesCh = make(chan *BareUri, 1)
	}
	return f.newskiesCh
}

func (f *Firehose) Posts() <-chan *PostRef {
	if f.postCh == nil {
		f.postCh = make(chan *PostRef, 1)
	}
	return f.postCh
}

func (f *Firehose) Reposts() <-chan *RepostRef {
	if f.repostCh == nil {
		f.repostCh = make(chan *RepostRef, 1)
	}
	return f.repostCh
}
