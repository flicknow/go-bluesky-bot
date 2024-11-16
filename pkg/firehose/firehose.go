package firehose

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"regexp"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	lexutil "github.com/bluesky-social/indigo/lex/util"
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

var DefaultJetstreamHost = "https://jetstream.atproto.tools"
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
	s *Jetstream
}

func NewFirehose(ctx context.Context) *Firehose {
	bgs, ok := ctx.Value("jetstream-host").(string)
	if !ok {
		bgs = DefaultJetstreamHost
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
	addr = addr + url.Host + "/subscribe"

	cursorPath, _ := ctx.Value("cursor").(string)

	return &Firehose{
		s: NewJetstream(addr, cursorPath),
	}
}

func (f *Firehose) Ack(seq int64) {
	f.s.Ack(seq)
}

func (f *Firehose) proxyStream(sCh <-chan *JetstreamEvent) <-chan *FirehoseEvent {
	fCh := make(chan *FirehoseEvent, ChannelBuffer)

	var jEvt *JetstreamEvent
	var lEvt *FirehoseEvent
	var seq int64
	lastSeq := f.s.cursor
	go func() {
		for jEvt = range sCh {
			lEvt = f.processJetstreamEventEvent(jEvt)
			if lEvt == nil {
				continue
			}

			seq = lEvt.Seq
			if (lastSeq == 0) && (lEvt.Seq != 0) {
				lastSeq = lEvt.Seq
			}

			if (seq != 0) && ((seq - lastSeq) > 300_000_000) {
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

func (f *Firehose) processJetstreamEventEvent(jEvt *JetstreamEvent) *FirehoseEvent {
	if jEvt.Error != nil {
		return &FirehoseEvent{Error: jEvt.Error, Type: EvtKindError}
	}

	body := jEvt.Body
	switch body.Kind {
	case "commit":
	default:
		return nil
	}

	var err error = nil
	commit := body.Commit
	seq := body.TimeUS
	if commit == nil {
		log.Printf("commit is nil: %s", utils.Dump(body))
		return nil
	}

	switch commit.Operation {
	case "create":
		uri := fmt.Sprintf("at://%s/%s/%s", body.Did, commit.Collection, commit.RKey)
		ref := &comatproto.RepoStrongRef{
			Cid: commit.CID,
			Uri: uri,
		}

		switch commit.Collection {
		case "app.bsky.feed.like":
			if body.Did != MARK {
				break
			}

			like := &appbsky.FeedLike{}
			err = json.Unmarshal(commit.Record, like)
			if err != nil {
				log.Printf("error decoding %s: %+v", uri, err)
				return nil
			}
			return &FirehoseEvent{Seq: seq, Like: &LikeRef{like, ref, seq}, Type: EvtKindFirehoseLike}
		case "app.bsky.feed.post":
			post := &appbsky.FeedPost{}
			err = json.Unmarshal(commit.Record, post)
			if err != nil {
				log.Printf("error decoding %s: %+v", uri, err)
				return nil
			}
			return &FirehoseEvent{Seq: seq, Post: NewPostRef(post, ref, seq), Type: EvtKindFirehosePost}
		case "app.bsky.actor.profile":
			return &FirehoseEvent{Seq: seq, Profile: body.Did, Type: EvtKindFirehoseProfile}
		}
	case "delete":
		return &FirehoseEvent{Delete: fmt.Sprintf("at://%s/%s/%s", body.Did, commit.Collection, commit.RKey), Type: EvtKindFirehoseDelete}
	}

	return nil
}
