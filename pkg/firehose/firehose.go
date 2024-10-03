package firehose

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strings"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/events"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/flicknow/go-bluesky-bot/pkg/utils"
)

const (
	EvtKindFirehoseHandle    = "#handle"
	EvtKindFirehoseIdentity  = "#identity"
	EvtKindFirehoseInfo      = "#info"
	EvtKindFirehoseMigrate   = "#migrate"
	EvtKindFirehoseTombstone = "#tombstone"
	EvtKindFirehoseDelete    = "#delete"
	EvtKindFirehoseBlock     = "#block"
	EvtKindFirehoseFollow    = "#follow"
	EvtKindFirehoseLike      = "#like"
	EvtKindFirehosePost      = "#post"
	EvtKindFirehoseProfile   = "#profile"
	EvtKindFirehoseRepost    = "#repost"
)

var DefaultBgsHost = "https://bsky.network"
var DmRegex = regexp.MustCompile(`(?i)^\W*DM\W*\s@\w+\.`)
var MARK = "did:plc:wzsilnxf24ehtmmc3gssy5bu"

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

func isHandledType(lextype string) bool {
	_, err := lexutil.NewFromType(lextype)
	return err == nil
}

type FirehoseEvent struct {
	Block     *BlockRef
	Delete    string
	Follow    *FollowRef
	Like      *LikeRef
	Post      *PostRef
	Profile   string
	Repost    *RepostRef
	Tombstone string
	Info      *comatproto.SyncSubscribeRepos_Info
	Error     error
	Seq       int64
	Type      string
}

type Firehose struct {
	s *Subscriber
}

func NewFirehose(ctx context.Context) *Firehose {
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
		log.Panicf("unsupported scheme %s for labeler %s", scheme, bgs)
	}
	addr = addr + url.Host + "/xrpc/com.atproto.sync.subscribeRepos"

	cursorPath, _ := ctx.Value("cursor").(string)

	return &Firehose{
		s: NewSubscriber(addr, cursorPath),
	}
}

func (f *Firehose) Ack(seq int64) {
	f.s.Ack(seq)
}

func (f *Firehose) proxyStream(sCh <-chan *SubscriberEvent) <-chan *FirehoseEvent {
	fCh := make(chan *FirehoseEvent, ChannelBuffer)

	var sEvt *SubscriberEvent
	var lEvt *FirehoseEvent
	var seq int64
	lastSeq := f.s.cursor
	go func() {
		for sEvt = range sCh {
			lEvt = f.processSubscriberEvent(sEvt)
			if lEvt == nil {
				continue
			}

			seq = lEvt.Seq
			if (lastSeq == 0) && (lEvt.Seq != 0) {
				lastSeq = lEvt.Seq
			}

			if (seq != 0) && ((seq - lastSeq) > 1000) {
				err := fmt.Errorf("%w: skipped too many seqs: went from %d to %d (%d)", ErrFatal, lastSeq, seq, seq-lastSeq)
				lEvt = &FirehoseEvent{Error: err, Type: EvtKindError}
			}

			fCh <- lEvt

			if seq != 0 {
				lastSeq = seq
			}

			if (lEvt.Error != nil) && (errors.Is(lEvt.Error, ErrFatal)) {
				break
			}
		}

		close(fCh)
	}()

	return fCh
}

func (f *Firehose) Start(ctx context.Context) (<-chan *FirehoseEvent, error) {
	ch, err := f.s.Start(ctx)
	if err != nil {
		return nil, err
	}

	return f.proxyStream(ch), nil
}

func (f *Firehose) Stop() {
	f.s.Stop()

}

func (f *Firehose) Restart(ctx context.Context) (<-chan *FirehoseEvent, error) {
	ch, err := f.s.Restart(ctx)
	if err != nil {
		return nil, err
	}

	return f.proxyStream(ch), nil
}

var CommitEvt comatproto.SyncSubscribeRepos_Commit

func (f *Firehose) processSubscriberEvent(sEvt *SubscriberEvent) *FirehoseEvent {
	if sEvt.Error != nil {
		return &FirehoseEvent{Error: sEvt.Error, Type: EvtKindError}
	}

	header := sEvt.Header
	if header.Op != events.EvtKindMessage {
		err := fmt.Errorf("unexpected header op: %d", header.Op)
		return &FirehoseEvent{Error: err, Type: EvtKindError}
	}

	switch header.MsgType {
	case "#handle":
		var evt comatproto.SyncSubscribeRepos_Handle
		if err := evt.UnmarshalCBOR(sEvt.Body); err != nil {
			return &FirehoseEvent{Error: fmt.Errorf("error unmarshalling #handle: %w", err), Type: EvtKindError}
		}
		return &FirehoseEvent{Seq: evt.Seq, Type: header.MsgType}
	case "#identity":
		var evt comatproto.SyncSubscribeRepos_Identity
		if err := evt.UnmarshalCBOR(sEvt.Body); err != nil {
			return &FirehoseEvent{Error: fmt.Errorf("error unmarshalling #identity: %w", err), Type: EvtKindError}
		}
		return &FirehoseEvent{Seq: evt.Seq, Type: header.MsgType}
	case "#info":
		var evt comatproto.SyncSubscribeRepos_Info
		if err := evt.UnmarshalCBOR(sEvt.Body); err != nil {
			return &FirehoseEvent{Error: fmt.Errorf("error unmarshalling #info: %w", err), Type: EvtKindError}
		}
		return &FirehoseEvent{Info: &evt, Type: header.MsgType}
	case "#migrate":
		var evt comatproto.SyncSubscribeRepos_Migrate
		if err := evt.UnmarshalCBOR(sEvt.Body); err != nil {
			return &FirehoseEvent{Error: fmt.Errorf("error unmarshalling #migrate: %w", err), Type: EvtKindError}
		}
		return &FirehoseEvent{Seq: evt.Seq, Type: header.MsgType}
	case "#tombstone":
		var evt comatproto.SyncSubscribeRepos_Tombstone
		if err := evt.UnmarshalCBOR(sEvt.Body); err != nil {
			return &FirehoseEvent{Error: fmt.Errorf("error unmarshalling #tombstone: %w", err), Type: EvtKindError}
		}
		return &FirehoseEvent{Tombstone: evt.Did, Seq: evt.Seq, Type: header.MsgType}
	case "#commit":
		ctx := f.s.conCtx

		if err := CommitEvt.UnmarshalCBOR(sEvt.Body); err != nil {
			return &FirehoseEvent{Error: fmt.Errorf("error unmarshalling #commit: %w", err), Type: EvtKindError}
		}

		if CommitEvt.TooBig {
			return nil
		}

		r, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(CommitEvt.Blocks))
		if err != nil {
			err = fmt.Errorf("reading repo from car (seq: %d, len: %d): %w", CommitEvt.Seq, len(CommitEvt.Blocks), err)
			return &FirehoseEvent{Error: err, Type: EvtKindError}
		}
		for _, op := range CommitEvt.Ops {
			path := op.Path
			parts := strings.SplitN(path, "/", 2)
			lextype := parts[0]
			uri := fmt.Sprintf("at://%s/%s", CommitEvt.Repo, path)
			if !isHandledType(lextype) {
				continue
			}
			if lextype == "app.bsky.feed.like" {
				if CommitEvt.Repo != MARK {
					continue
				}
			} else if lextype == "app.bsky.graph.follow" {
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
					err := fmt.Errorf("mismatch in record and op cid: %s != %s", rc, *op.Cid)
					return &FirehoseEvent{Error: err, Type: EvtKindError}
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
					return &FirehoseEvent{Seq: CommitEvt.Seq, Like: &LikeRef{like, ref, CommitEvt.Seq}, Type: EvtKindFirehoseLike}
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
					return &FirehoseEvent{Seq: CommitEvt.Seq, Post: NewPostRef(post, ref, CommitEvt.Seq), Type: EvtKindFirehosePost}
					/*
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
							return &FirehoseEvent{Seq: evt.Seq, Repost: &RepostRef{repost, ref, evt.Seq}, Type: EvtKindFirehoseRepost}
					*/
				case "app.bsky.actor.profile":
					if ek == repomgr.EvtKindCreateRecord {
						return &FirehoseEvent{Seq: CommitEvt.Seq, Profile: CommitEvt.Repo, Type: EvtKindFirehoseProfile}
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
					return &FirehoseEvent{Seq: CommitEvt.Seq, Block: &BlockRef{block.Subject, ref, CommitEvt.Seq}, Type: EvtKindFirehoseBlock}
					/*
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
							return &FirehoseEvent{Seq: evt.Seq, Follow: &FollowRef{follow.Subject, ref, evt.Seq}, Type: EvtKindFirehoseFollow}
					*/
				}
			case repomgr.EvtKindDeleteRecord:
				return &FirehoseEvent{Seq: CommitEvt.Seq, Delete: uri, Type: EvtKindFirehoseDelete}
			}
		}

	}

	return nil
}
