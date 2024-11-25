package dbx

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/crypto"
	"github.com/flicknow/go-bluesky-bot/pkg/clock"
	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/firehose"
	"github.com/flicknow/go-bluesky-bot/pkg/metrics"
	"github.com/flicknow/go-bluesky-bot/pkg/utils"
	"github.com/jmoiron/sqlx"
	sqlite3 "github.com/mattn/go-sqlite3"
)

var DefaultDbDir = fmt.Sprintf("%s/.bsky/db", os.Getenv("HOME"))
var DefaultDbActorCacheSize = 500000
var DefaultDbLabelCacheSize = 500000
var DefaultDbFollowCacheSize = 50000

var LabelerDid = "did:plc:jcce2sa3fgue4wiocvf7e7xj"
var AMELIA_BUT_ALSO_ITS_REM = "did:plc:6gwchzxwoj7jms5nilauupxq"
var MARK = "did:plc:wzsilnxf24ehtmmc3gssy5bu"
var REM = "did:plc:asb3rgscdkkv636buq6blof6"
var SKYTAN = "did:plc:ikvaup2d6nlir7xfm5vgzvra"
var ʕ٠ᴥ٠ʔ = "did:plc:nhvvwh2qglcmsbvba7durp7f"
var SQLiteMaxInt int64 = 9223372036854775807
var PinnedFollowPostUrl = "at://did:plc:wzsilnxf24ehtmmc3gssy5bu/app.bsky.feed.post/3kexw5q5mix22"
var PinnedFollowPost *PostRow = nil
var Collector *metrics.Collector = metrics.NewCollector(len(os.Getenv("GO_BLUESKY_METRICS")) > 0)
var SlowQueryThresholdMs int64 = 1000
var SQLiteDriver = "sqlite3_default"
var SQLiteMMapSize = 0
var SQLiteWalAutocheckpoint = 0
var SQLiteSynchronous = "NORMAL"

var BangerRegex = regexp.MustCompile(`^\W*banger\b`)

type DBx struct {
	Actors           *DBxTableActors
	CustomLabels     *DBxTableCustomLabels
	Dms              *DBxTableDms
	Follows          *DBxTableFollows
	FollowsIndexed   *DBxTableFollowsIndexed
	Labels           *DBxTableLabels
	Likes            *DBxTableLikes
	Mentions         *DBxTableMentions
	Posts            *DBxTablePosts
	PostLabels       *DBxTablePostLabels
	Quotes           *DBxTableQuotes
	Replies          *DBxTableReplies
	Reposts          *DBxTableReposts
	ThreadMentions   *DBxTableThreadMentions
	clock            clock.Clock
	debug            bool
	extendedIndexing bool
	SigningKey       *crypto.PrivateKeyK256
}

func (d *DBx) Block(did string) error {
	actor, err := d.Actors.FindOrCreateActor(did)
	if err != nil {
		return err
	}

	actor.Blocked = true
	err = d.InitActorInfo(actor, []*PostLabelRow{})
	if err != nil {
		return err
	}

	return nil
}

type LabelDef struct {
	PostLabelId int64
	CreatedAt   int64
	Uri         string
	Val         string
}

func uniqueStrings(items []string) []string {
	unique := make([]string, 0, len(items))

	seen := make(map[string]bool)
	for _, item := range items {
		if saw := seen[item]; !saw {
			unique = append(unique, item)
			seen[item] = true
		}
	}

	return unique
}

func uniqueInt64s(items []int64) []int64 {
	unique := make([]int64, 0, len(items))

	seen := make(map[int64]bool)
	for _, item := range items {
		if saw := seen[item]; !saw {
			unique = append(unique, item)
			seen[item] = true
		}
	}

	return unique
}

func ParallelizeFuncsWithRetries(funcs ...func() error) []error {
	retryableFuncs := make([]func() error, len(funcs))
	for i, f := range funcs {
		retryableFuncs[i] = RetryDbIsLocked(f)
	}

	return utils.ParallelizeFuncs(retryableFuncs...)
}

func RetryDbIsLocked(f func() error) func() error {
	return func() error {
		var err error = nil
		for i := 0; i < 5; i++ {
			err = f()
			if err == nil {
				return nil
			} else if !(errors.Is(err, sqlite3.ErrBusy) || errors.Is(err, sqlite3.ErrLocked) || strings.Contains(err.Error(), "database is locked")) {
				fmt.Printf("> RETRY RETURNING ERR: %+v\n", err)
				return err
			}
			fmt.Printf("> RETRY %d: %+v\n", i+1, err)
		}
		fmt.Printf("> OUT OF RETRIES RETURNING ERR: %+v\n", err)
		return err
	}
}

func (d *DBx) InsertLike(likeRef *firehose.LikeRef) error {
	uri := likeRef.Ref.Uri
	did := utils.ParseDid(uri)
	isMark := did == MARK

	if !(d.extendedIndexing || isMark) {
		return nil
	}

	deferredActorId := NewDeferredInt64()
	deferredPost := NewDeferredPost()

	errs := ParallelizeFuncsWithRetries(
		func() error {
			defer deferredActorId.Cancel()

			actor, err := d.Actors.FindOrCreateActor(utils.ParseDid(uri))
			if err != nil {
				return err
			}

			deferredActorId.Done(actor.ActorId)

			return nil
		},
		func() error {
			defer deferredPost.Cancel()

			post, err := d.Posts.FindByUri(likeRef.Like.Subject.Uri)
			if err != nil {
				return err
			}

			deferredPost.Done(post)

			return nil
		},
		func() error {
			actorid := deferredActorId.Get()
			post := deferredPost.Get()
			if (actorid == 0) || (post == nil) {
				return nil
			}

			likerow := &LikeRow{
				ActorId:       actorid,
				DehydratedUri: utils.DehydrateUri(uri),
				Uri:           uri,
				SubjectId:     post.PostId,
				CreatedAt:     d.clock.NowUnix(),
			}

			_, err := d.Likes.NamedExec("INSERT INTO likes (actor_id, uri, subject_id, created_at) VALUES (:actor_id, :uri, :subject_id, :created_at)", likerow)
			if err != nil {
				return err
			}

			return nil
		},
		func() error {
			post := deferredPost.Get()
			if post == nil {
				return nil
			}

			postid := post.PostId
			_, err := d.Posts.Exec("UPDATE posts SET likes = likes + 1 WHERE post_id = ?", postid)
			if err != nil {
				log.Printf("ERROR updating like count for post %d: %+v\n", postid, err)
			}

			return nil
		},
		func() error {
			if !isMark {
				return nil
			}

			post := deferredPost.Get()
			if post == nil {
				return nil
			}

			banger, err := d.Labels.FindOrCreateLabel("banger")
			if err != nil {
				log.Printf("ERROR retrieving banger label: %+v\n", err)
				return nil
			}

			now := d.clock.NowUnix()
			ver := int64(1)
			label := &atproto.LabelDefs_Label{
				Cts: time.Unix(now, 0).UTC().Format(time.RFC3339),
				Src: LabelerDid,
				Uri: utils.HydrateUri(post.DehydratedUri, "app.bsky.feed.post"),
				Val: "banger",
				Ver: &ver,
			}

			sigBuf := new(bytes.Buffer)
			err = label.MarshalCBOR(sigBuf)
			if err != nil {
				return err
			}

			sigBytes, err := d.SigningKey.HashAndSign(sigBuf.Bytes())
			if err != nil {
				return err
			}
			label.Sig = sigBytes

			cborBuf := new(bytes.Buffer)
			err = label.MarshalCBOR(cborBuf)
			if err != nil {
				return err
			}

			row := &CustomLabel{
				SubjectType: PostLabelType,
				SubjectId:   post.PostId,
				CreatedAt:   now,
				LabelId:     banger.LabelId,
				Neg:         0,
				Cbor:        cborBuf.Bytes(),
			}

			return d.CustomLabels.InsertLabels([]*CustomLabel{row})
		},
	)
	if len(errs) > 0 {
		msg := fmt.Sprintf("Error indexing like %s:", uri)
		for _, e := range errs {
			msg = fmt.Sprintf("%s\n%s", msg, e)
		}
		log.Print(msg)
		return errors.New(msg)
	}

	return nil
}

func (d *DBx) InsertRepost(repostRef *firehose.RepostRef) error {
	if !d.extendedIndexing {
		return nil
	}

	uri := repostRef.Ref.Uri

	deferredActorId := NewDeferredInt64()
	deferredPostId := NewDeferredInt64()

	errs := ParallelizeFuncsWithRetries(
		func() error {
			defer deferredActorId.Cancel()

			actor, err := d.Actors.FindOrCreateActor(utils.ParseDid(uri))
			if err != nil {
				return err
			}

			deferredActorId.Done(actor.ActorId)

			return nil
		},
		func() error {
			defer deferredPostId.Cancel()

			postid, err := d.Posts.FindPostIdByUri(repostRef.Repost.Subject.Uri)
			if err != nil {
				return err
			}

			deferredPostId.Done(postid)

			return nil
		},
		func() error {
			actorid := deferredActorId.Get()
			postid := deferredPostId.Get()
			if (actorid == 0) || (postid == 0) {
				return nil
			}

			repostrow := &RepostRow{
				ActorId:       actorid,
				DeHydratedUri: utils.DehydrateUri(uri),
				Uri:           uri,
				SubjectId:     postid,
				CreatedAt:     d.clock.NowUnix(),
			}

			_, err := d.Reposts.NamedExec("INSERT INTO reposts (actor_id, uri, subject_id, created_at) VALUES (:actor_id, :uri, :subject_id, :created_at)", repostrow)
			if err != nil {
				return err
			}

			return nil
		},
		func() error {
			postid := deferredPostId.Get()
			if postid == 0 {
				return nil
			}

			_, err := d.Posts.Exec("UPDATE posts SET reposts = reposts + 1 WHERE post_id = ?", postid)
			if err != nil {
				log.Printf("ERROR updating repost count for post %d: %+v\n", postid, err)
			}

			return nil
		},
	)
	if len(errs) > 0 {
		msg := fmt.Sprintf("Error indexing repost %s:", uri)
		for _, e := range errs {
			msg = fmt.Sprintf("%s\n%s", msg, e)
		}
		log.Print(msg)
		return errors.New(msg)
	}

	return nil
}

func (d *DBx) InsertPost(postRef *firehose.PostRef, actorRow *ActorRow, labels ...string) (*PostRow, error) {
	/*
		clock := d.clock
		startMethod := clock.NowUnixMilli()
		metric := metrics.NewInsertPost()
		defer func() {
			metric.Val = clock.NowUnixMilli() - startMethod
			Collector.InsertPost(metric)
		}()
	*/

	post := postRef.Post
	quote := postRef.Quotes
	uri := postRef.Ref.Uri
	now := d.clock.NowUnix()

	createdAt := postRef.Post.CreatedAt
	if createdAt == "" {
		return nil, fmt.Errorf("post %s has no createdAt", uri)
	}
	t, err := time.Parse(time.RFC3339, createdAt)
	if err != nil {
		return nil, err
	}
	if t.UTC().Unix() < (now - 604800) {
		return nil, nil
	}

	if (post.Labels != nil) && (post.Labels.LabelDefs_SelfLabels != nil) {
		for _, selfLabel := range post.Labels.LabelDefs_SelfLabels.Values {
			labels = append(labels, selfLabel.Val)
		}
	}

	parentDid := ""
	parentUri := ""
	ignorePinReply := false
	if (post.Reply != nil) && (post.Reply.Parent != nil) {
		parentUri = post.Reply.Parent.Uri
		parentDid = utils.ParseDid(parentUri)
		ignorePinReply = (post.Text == "📌") && (parentDid == SKYTAN)
	}

	deferredPostid := NewDeferredInt64()
	deferredParent := NewDeferredPost()
	deferredQuoted := NewDeferredPost()
	//deferredRoot := NewDeferredPost()
	deferredParentActor := NewDeferredInt64()
	deferredQuotedActor := NewDeferredInt64()
	deferredMentionedActors := NewDeferredInt64s()

	var postRow *PostRow = nil
	errs := ParallelizeFuncsWithRetries(
		func() error {
			defer deferredPostid.Cancel()
			//defer func() { metric.Insert = clock.NowUnixMilli() - startMethod }()

			postRow = &PostRow{
				Uri:           uri,
				DehydratedUri: utils.DehydrateUri(uri),
				ActorId:       actorRow.ActorId,
				CreatedAt:     now,
				Labeled:       0,
			}
			if !postRef.HasMedia() {
				postRow.Labeled = 1
			}
			if actorRow.Blocked {
				postRow.Labeled = 1
			}

			_, err := d.Posts.InsertPost(postRow)
			if err != nil {
				return err
			}

			deferredPostid.Done(postRow.PostId)

			return nil
		},
		func() error {
			defer func() {
				//metric.FindPosts = clock.NowUnixMilli() - startMethod
				deferredParent.Cancel()
				deferredQuoted.Cancel()
				//deferredRoot.Cancel()
			}()

			//rootUri := ""

			uris := make([]string, 0, 3)
			if post.Reply != nil {
				if post.Reply.Parent != nil {
					uris = append(uris, parentUri)
				}
				/*
					if post.Reply.Root != nil {
						rootUri = post.Reply.Root.Uri
						uris = append(uris, rootUri)
					}
				*/
			}

			if quote != "" {
				uris = append(uris, quote)
			}

			if len(uris) == 0 {
				return nil
			}

			posts, err := d.Posts.FindByUris(uris)
			if err != nil {
				return err
			}
			for _, post := range posts {
				uri := post.Uri
				if parentUri == uri {
					deferredParent.Done(post)
				}
				/*
					if rootUri == uri {
						deferredRoot.Done(post)
					}
				*/
				if quote == uri {
					deferredQuoted.Done(post)
				}
			}

			return nil
		},
		func() error {
			if (post.Reply == nil) || (post.Reply.Parent == nil) {
				return nil
			}

			//defer func() { metric.InsertReply.Val = clock.NowUnixMilli() - startMethod }()

			if ignorePinReply {
				return nil
			}

			if parentUri == "" {
				return nil
			}

			parentRow := deferredParent.Get()
			if parentRow == nil {
				parentActor := deferredParentActor.Get()
				if parentActor == 0 {
					return nil
				}

				parentRow = &PostRow{
					ActorId: parentActor,
					Uri:     parentUri,
				}
			}

			/*
				rootRow := deferredRoot.Get()
				if rootRow == nil {
					rootRow = &PostRow{}
				}
			*/

			postid := deferredPostid.Get()
			if postid == 0 {
				return nil
			}

			//startInsert := clock.NowUnixMilli()
			replyRow := &ReplyRow{
				PostId:        postid,
				ActorId:       actorRow.ActorId,
				ParentId:      parentRow.PostId,
				ParentActorId: parentRow.ActorId,
				//RootId:        rootRow.PostId,
				//RootActorId:   rootRow.ActorId,
			}

			e := d.Replies.InsertReply(replyRow)
			//metric.InsertReply.Insert = clock.NowUnixMilli() - startInsert
			if e != nil {
				return e
			}

			return nil
		},
		func() error {
			defer func() {
				//metric.FindMentionedAndQuotedActors = clock.NowUnixMilli() - startMethod
				deferredParentActor.Cancel()
				deferredQuotedActor.Cancel()
				deferredMentionedActors.Cancel()
			}()

			dids := make([]string, 0, len(postRef.Mentions)+1)

			parentDid := ""
			if parentUri != "" {
				parentDid = utils.ParseDid(parentUri)
				dids = append(dids, parentDid)
			}

			quotedDid := ""
			if quote != "" {
				quotedDid = utils.ParseDid(quote)
				dids = append(dids, quotedDid)
			}

			isMentioned := make(map[string]bool)
			mentionedDids := uniqueStrings(postRef.Mentions)
			for _, did := range mentionedDids {
				dids = append(dids, did)
				isMentioned[did] = true
			}

			if len(dids) == 0 {
				return nil
			}

			actors, err := d.Actors.FindOrCreateActors(uniqueStrings(dids))
			if err != nil {
				return nil
			}

			mentionedActorIds := make([]int64, 0, len(mentionedDids))
			for _, actor := range actors {
				did := actor.Did
				id := actor.ActorId
				if parentDid == did {
					deferredParentActor.Done(id)
				}
				if quotedDid == did {
					deferredQuotedActor.Done(id)
				}
				if isMentioned[did] {
					mentionedActorIds = append(mentionedActorIds, id)
				}
			}
			deferredMentionedActors.Done(mentionedActorIds)

			return nil
		},
		func() error {
			if quote == "" {
				return nil
			}

			//defer func() { metric.InsertQuote.Val = clock.NowUnixMilli() - startMethod }()

			if quote == "" {
				return nil
			}

			quotedpost := deferredQuoted.Get()
			if quotedpost == nil {
				quotedactor := deferredQuotedActor.Get()
				if quotedactor == 0 {
					return nil
				}

				quotedpost = &PostRow{
					ActorId: quotedactor,
					Uri:     quote,
				}
			}

			postid := deferredPostid.Get()
			if postid == 0 {
				return nil
			}

			//startInsert := clock.NowUnixMilli()
			quoterow := &QuoteRow{
				PostId:         postid,
				ActorId:        actorRow.ActorId,
				SubjectId:      quotedpost.PostId,
				SubjectActorId: quotedpost.ActorId,
			}
			e := d.Quotes.InsertQuote(quoterow)
			//metric.InsertQuote.Insert = clock.NowUnixMilli() - startInsert
			if e != nil {
				return e
			}

			return nil
		},
		func() error {
			postMentions := deferredMentionedActors.Get()
			if (postMentions == nil) || (len(postMentions) == 0) {
				return nil
			}

			postid := deferredPostid.Get()
			if postid == 0 {
				return nil
			}

			//startInsert := clock.NowUnixMilli()
			e := d.Mentions.InsertMentions(postid, actorRow.ActorId, postMentions)
			if e != nil {
				return e
			}
			//metric.InsertMentions.Insert = clock.NowUnixMilli() - startInsert

			return nil
		},
		func() error {
			if len(labels) == 0 {
				return nil
			}
			//defer func() { metric.InsertLabels.Val = clock.NowUnixMilli() - startMethod }()

			labelids := make([]int64, 0, len(labels))
			for _, name := range labels {
				label, err := d.Labels.FindOrCreateLabel(name)
				if err != nil {
					return err
				}

				labelids = append(labelids, label.LabelId)
			}
			//metric.InsertLabels.FindLabels = clock.NowUnixMilli() - startMethod

			postid := deferredPostid.Get()
			if postid == 0 {
				return nil
			}

			//startInsert := clock.NowUnixMilli()
			//defer func() { metric.InsertLabels.Insert = clock.NowUnixMilli() - startInsert }()

			return d.PostLabels.InsertPostLabel(postid, labelids)
		},
		/*
			func() error {
				if !postRef.IsDm() && ((post.Reply == nil) || (post.Reply.Parent == nil)) {
					return nil
				}
				//defer func() { metric.InsertDMs.Val = clock.NowUnixMilli() - startMethod }()

				actorids := make([]int64, 0)
				parent := deferredParent.Get()
				if parent != nil {
					//startSelect := clock.NowUnixMilli()
					dmMentions, err := d.Dms.SelectDms(parent.PostId)
					//metric.InsertDMs.FindDMs = clock.NowUnixMilli() - startSelect
					if err != nil {
						return err
					}
					for _, actorid := range dmMentions {
						actorids = append(actorids, actorid)
					}
				}

				if !postRef.IsDm() && (len(actorids) == 0) {
					return nil
				}

				actorids = append(actorids, actorRow.ActorId)

				for _, actorid := range deferredMentionedActors.Get() {
					actorids = append(actorids, actorid)
				}

				postid := deferredPostid.Get()
				if postid == 0 {
					return nil
				}

				//startInsert := clock.NowUnixMilli()
				e := d.Dms.InsertDms(postid, uniqueInt64s(actorids))
				if e != nil {
					return e
				}
				//metric.InsertDMs.Insert = clock.NowUnixMilli() - startInsert

				return nil
			},
				func() error {
					//defer func() { metric.InsertThreadMentions.Val = clock.NowUnixMilli() - startMethod }()
					actorids := make([]int64, 0)

					if parentUri != "" {
						parent := deferredParent.Get()
						if parent != nil {
							if parent.ActorId != actorRow.ActorId {
								actorids = append(actorids, parent.ActorId)
							}

							//startSelect := clock.NowUnixMilli()
							parentmentions, err := d.ThreadMentions.SelectThreadMentions(parent.PostId)
							//metric.InsertThreadMentions.FindThreadMentions = clock.NowUnixMilli() - startSelect
							if err != nil {
								return err
							}
							for _, actorid := range parentmentions {
								if actorid == actorRow.ActorId {
									continue
								}
								actorids = append(actorids, actorid)
							}
						} else {
							parentActor := deferredParentActor.Get()
							if parentActor != 0 {
								actorids = append(actorids, parentActor)
							}
						}
					}

					for _, actorid := range deferredMentionedActors.Get() {
						if actorid == actorRow.ActorId {
							continue
						}
						actorids = append(actorids, actorid)
					}

					quotedactorid := deferredQuotedActor.Get()
					if (quotedactorid != 0) && (quotedactorid != actorRow.ActorId) {
						actorids = append(actorids, quotedactorid)
					}

					postid := deferredPostid.Get()
					if postid == 0 {
						return nil
					}

					//startInsert := clock.NowUnixMilli()
					e := d.ThreadMentions.InsertThreadMention(postid, uniqueInt64s(actorids))
					if e != nil {
						return e
					}
					//metric.InsertThreadMentions.Insert = clock.NowUnixMilli() - startInsert
					return nil
				},
		*/
		func() error {
			if !((actorRow.ActorId != 0) && ((post.Reply == nil) || (post.Reply.Parent == nil))) {
				return nil
			}
			//defer func() { metric.UpdateActor = clock.NowUnixMilli() - startMethod }()

			postid := deferredPostid.Get()
			if postid == 0 {
				return nil
			}

			err := d.Actors.IncrementPostsCount(actorRow, now)
			if err != nil {
				log.Printf("ERROR updating post count for actor %d: %+v\n", actorRow.ActorId, err)
				return nil
			}

			return nil
		},
		func() error {
			did := utils.ParseDid(uri)
			if did != ʕ٠ᴥ٠ʔ {
				return nil
			}

			if !BangerRegex.MatchString(post.Text) {
				return nil
			}

			parent := deferredParent.Get()
			if parent == nil {
				return nil
			}

			banger, err := d.Labels.FindOrCreateLabel("banger")
			if err != nil {
				log.Printf("ERROR retrieving banger label: %+v\n", err)
				return nil
			}
			now := d.clock.NowUnix()
			ver := int64(1)
			label := &atproto.LabelDefs_Label{
				Cts: time.Unix(now, 0).UTC().Format(time.RFC3339),
				Src: LabelerDid,
				Uri: parentUri,
				Val: "banger",
				Ver: &ver,
			}

			sigBuf := new(bytes.Buffer)
			err = label.MarshalCBOR(sigBuf)
			if err != nil {
				return err
			}

			sigBytes, err := d.SigningKey.HashAndSign(sigBuf.Bytes())
			if err != nil {
				return err
			}
			label.Sig = sigBytes

			cborBuf := new(bytes.Buffer)
			err = label.MarshalCBOR(cborBuf)
			if err != nil {
				return err
			}

			row := &CustomLabel{
				SubjectType: PostLabelType,
				SubjectId:   parent.PostId,
				CreatedAt:   now,
				LabelId:     banger.LabelId,
				Cbor:        cborBuf.Bytes(),
			}

			return d.CustomLabels.InsertLabels([]*CustomLabel{row})
		},
		/*
			func() error {
				if !d.extendedIndexing {
					return nil
				}

				quotedPost := deferredQuoted.Get()
				if quotedPost == nil {
					return nil
				}

				quotedPostId := quotedPost.PostId
				_, err := d.Posts.Exec("UPDATE posts SET quotes = quotes + 1 WHERE post_id = ?", quotedPostId)
				if err != nil {
					log.Printf("ERROR updating quotes count for post %d: %+v\n", quotedPostId, err)
				}

				return nil
			},
			func() error {
				if !d.extendedIndexing {
					return nil
				}

				parent := deferredParent.Get()
				if (parent == nil) || (parent.PostId == 0) {
					return nil
				}

				_, err := d.Posts.Exec("UPDATE posts SET replies = replies + 1 WHERE post_id = ?", parent.PostId)
				if err != nil {
					log.Printf("ERROR updating replies count for post %d: %+v\n", parent.PostId, err)
				}

				return nil
			},
		*/
	)
	if len(errs) > 0 {
		msg := fmt.Sprintf("Error indexing post %s:", uri)
		for _, e := range errs {
			msg = fmt.Sprintf("%s\n%s", msg, e)
		}
		log.Print(msg)
		return nil, errors.New(msg)
	}

	return postRow, nil
}

func DbExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("error checking if db %s exists: %w", path, err)
	} else if info.Size() == 0 {
		return false, nil
	} else if info.IsDir() {
		return false, fmt.Errorf("expected db path %s is a directory", path)
	}
	return true, nil
}

func SQLxMustOpen(path string, initsql string) *sqlx.DB {
	db, err := SQLxOpen(path, initsql)
	if err != nil {
		panic(err)
	}
	return db
}

func SQLxOpen(path string, initsql string) (*sqlx.DB, error) {
	exists, err := DbExists(path)
	if err != nil {
		return nil, fmt.Errorf("error checking if sqlite db %s exists: %w", path, err)
	}

	pool, err := sqlx.Open(SQLiteDriver, fmt.Sprintf("file:%s?_busy_timeout=10000&_journal_mode=WAL&_txlock=immediate&_synchronous=%s&_mmap_size=%d", path, SQLiteSynchronous, SQLiteMMapSize))
	if err != nil {
		return nil, fmt.Errorf("cannot open sqlite file %s: %w", path, err)
	}

	if SQLiteWalAutocheckpoint > 0 {
		_, err = pool.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", SQLiteWalAutocheckpoint))
		if err != nil {
			return nil, err
		}
	}

	if !exists {
		_, err := pool.Exec(initsql)
		if err != nil {
			return nil, fmt.Errorf("cannot initialize sqlite db %s: %w", path, err)
		}
	}

	return pool, nil
}

func concatInt64s(slices ...[]int64) []int64 {
	joined := make([]int64, 0, len(slices))

	for _, slice := range slices {
		for _, val := range slice {
			joined = append(joined, val)
		}
	}

	return joined
}

func concatPosts(slices ...[]*PostRow) []*PostRow {
	joined := make([]*PostRow, 0, len(slices))

	for _, slice := range slices {
		for _, val := range slice {
			joined = append(joined, val)
		}
	}

	return joined
}

func removeActorId(posts []*PostRow, actorid int64) []*PostRow {
	cleaned := make([]*PostRow, 0, len(posts))
	for _, post := range posts {
		if post.ActorId != actorid {
			cleaned = append(cleaned, post)
		}
	}
	return cleaned
}

func sortByPostIdDesc(posts []*PostRow) []*PostRow {
	sort.SliceStable(posts, func(i, j int) bool { return posts[j].PostId <= posts[i].PostId })
	return posts
}

func (d *DBx) DeletePost(uri string) error {
	/*
		clock := d.clock
		startMethod := clock.NowUnixMilli()
		metric := metrics.NewDeletePost()
		defer func() {
			metric.Val = clock.NowUnixMilli() - startMethod
			Collector.DeletePost(metric)
		}()
	*/

	postrow, err := d.Posts.FindByUri(uri)
	if err != nil {
		return err
	} else if (postrow == nil) || (postrow.PostId == 0) {
		return nil
	}
	postid := postrow.PostId
	deferredQuotedId := NewDeferredInt64()
	deferredParentId := NewDeferredInt64()

	errs := ParallelizeFuncsWithRetries(
		/*
			func() error {
				if !d.extendedIndexing {
					return nil
				}

				exists, err := queryHasResults(d.Likes, "SELECT 1 FROM likes WHERE subject_id = $1", postid)
				if err != nil {
					return err
				} else if !exists {
					return nil
				}

				_, err = d.Likes.Exec("DELETE FROM likes WHERE subject_id = $1", postid)
				return err
			},
			func() error {
				if !d.extendedIndexing {
					return nil
				}

				exists, err := queryHasResults(d.Reposts, "SELECT 1 FROM reposts WHERE subject_id = $1", postid)
				if err != nil {
					return err
				} else if !exists {
					return nil
				}

				_, err = d.Reposts.Exec("DELETE FROM reposts WHERE subject_id = $1", postid)
				return err
			},
		*/
		func() error {
			//defer func() { metric.DeleteMentions.Val = clock.NowUnixMilli() - startMethod }()

			mentions, err := d.Mentions.SelectMentions(postid)
			if err != nil {
				return err
			}
			if len(mentions) == 0 {
				return nil
			}
			//metric.DeleteMentions.FindMentions = clock.NowUnixMilli() - startMethod

			//startDelete := clock.NowUnixMilli()
			err = d.Mentions.DeleteMentionByPostId(postid)
			//metric.DeleteMentions.Delete = clock.NowUnixMilli() - startDelete
			if err != nil {
				return err
			}
			return nil
		},
		/*
				func() error {
					//defer func() { metric.DeleteThreadMentions.Val = clock.NowUnixMilli() - startMethod }()

					mentions, err := d.ThreadMentions.SelectThreadMentions(postid)
					if err != nil {
						return err
					}
					if len(mentions) == 0 {
						return nil
					}
					//metric.DeleteThreadMentions.FindThreadMentions = clock.NowUnixMilli() - startMethod

					//startDelete := clock.NowUnixMilli()
					err = d.ThreadMentions.DeleteThreadMentionsByPostId(postid)
					//metric.DeleteThreadMentions.Delete = clock.NowUnixMilli() - startDelete
					if err != nil {
						return err
					}
					return nil
				},
			func() error {
				//defer func() { metric.DeleteDMs.Val = clock.NowUnixMilli() - startMethod }()

				dms, err := d.Dms.SelectDms(postid)
				if err != nil {
					return err
				}
				if len(dms) == 0 {
					return nil
				}
				//metric.DeleteDMs.FindDMs = clock.NowUnixMilli() - startMethod

				//startDelete := clock.NowUnixMilli()
				err = d.Dms.DeleteDmsByPostId(postid)
				//metric.DeleteDMs.Delete = clock.NowUnixMilli() - startDelete
				if err != nil {
					return err
				}
				return nil
			},
		*/
		func() error {
			//defer func() { metric.DeleteLabels.Val = clock.NowUnixMilli() - startMethod }()

			postlabels, err := d.PostLabels.SelectLabelsByPostId(postid)
			if err != nil {
				return err
			}
			if len(postlabels) == 0 {
				return nil
			}
			//metric.DeleteLabels.FindLabels = clock.NowUnixMilli() - startMethod

			//startDelete := clock.NowUnixMilli()
			err = d.PostLabels.DeletePostLabelsByPostId(postid)
			//metric.DeleteLabels.Delete = clock.NowUnixMilli() - startDelete
			if err != nil {
				return err
			}
			return nil
		},
		func() error {
			defer deferredParentId.Cancel()
			//defer func() { metric.DeleteReply.Val = clock.NowUnixMilli() - startMethod }()

			reply, err := d.Replies.FindByPostId(postid)
			if (err != nil) && !errors.Is(err, sql.ErrNoRows) {
				return err
			} else if err != nil {
				return nil
			}
			deferredParentId.Done(reply.ParentId)
			//metric.DeleteReply.FindReply = clock.NowUnixMilli() - startMethod

			//startDelete := clock.NowUnixMilli()
			err = d.Replies.DeleteReply(reply.ReplyId)
			//metric.DeleteReply.Delete = clock.NowUnixMilli() - startDelete
			if err != nil {
				return err
			}

			return nil
		},
		func() error {
			defer deferredQuotedId.Cancel()
			//defer func() { metric.DeleteQuote.Val = clock.NowUnixMilli() - startMethod }()

			quote, err := d.Quotes.FindByPostId(postid)
			if (err != nil) && !errors.Is(err, sql.ErrNoRows) {
				return err
			} else if err != nil {
				return nil
			}
			deferredQuotedId.Done(quote.SubjectId)
			//metric.DeleteQuote.FindQuote = clock.NowUnixMilli() - startMethod

			//startDelete := clock.NowUnixMilli()
			err = d.Quotes.DeleteQuote(quote.QuoteId)
			//metric.DeleteQuote.Delete = clock.NowUnixMilli() - startDelete
			if err != nil {
				return err
			}
			return nil
		},
	)
	if len(errs) > 0 {
		msg := fmt.Sprintf("Error deleting metadata for post %s:", uri)
		for _, e := range errs {
			msg = fmt.Sprintf("%s\n%s", msg, e)
		}
		log.Print(msg)
		return errors.New(msg)
	}

	errs = ParallelizeFuncsWithRetries(
		func() error {
			//defer func() { metric.Delete = clock.NowUnixMilli() - startMethod }()
			err := d.Posts.DeletePost(postid)
			if err != nil {
				return err
			}
			return nil
		},
		func() error {
			if deferredParentId.Get() != 0 {
				return nil
			}
			//defer func() { metric.UpdateActor = clock.NowUnixMilli() - startMethod }()

			err := d.Actors.DecrementPostsCount(postrow.ActorId, utils.ParseDid(uri))
			if err != nil {
				return err
			}

			return nil
		},
		/*
			func() error {
				if !d.extendedIndexing {
					return nil
				}

				quotedId := deferredQuotedId.Get()
				if quotedId == 0 {
					return nil
				}

				_, err := d.Posts.Exec("UPDATE posts SET quotes = quotes - 1 WHERE post_id = $1", quotedId)
				return err
			},
			func() error {
				if !d.extendedIndexing {
					return nil
				}

				parentId := deferredParentId.Get()
				if parentId == 0 {
					return nil
				}

				_, err := d.Posts.Exec("UPDATE posts SET replies = replies - 1 WHERE post_id = $1", postid)
				return err
			},
		*/
	)
	if len(errs) > 0 {
		msg := fmt.Sprintf("Error deleting post %s:", uri)
		for _, e := range errs {
			msg = fmt.Sprintf("%s\n%s", msg, e)
		}
		log.Print(msg)
		return errors.New(msg)
	}

	return nil
}

func (d *DBx) DeleteLike(uri string) error {
	did := utils.ParseDid(uri)
	isMark := did == MARK

	if !(d.extendedIndexing || isMark) {
		return nil
	}

	likerow := &LikeRow{}
	err := d.Likes.Get(likerow, "SELECT * FROM likes WHERE uri = ?", utils.DehydrateUri(uri))
	if errors.Is(err, sql.ErrNoRows) || (likerow.LikeId == 0) {
		return nil
	} else if err != nil {
		return err
	}

	errs := ParallelizeFuncsWithRetries(
		func() error {
			_, err := d.Likes.Exec("DELETE FROM likes WHERE like_id = $1", likerow.LikeId)
			return err
		},
		func() error {
			_, err := d.Posts.Exec("UPDATE posts SET likes = likes - 1 WHERE post_id = $1", likerow.SubjectId)
			return err
		},
		func() error {
			if !isMark {
				return nil
			}

			banger, err := d.Labels.FindOrCreateLabel("banger")
			if err != nil {
				log.Printf("ERROR retrieving banger label: %+v\n", err)
				return nil
			}

			posts, err := d.Posts.SelectPostsById([]int64{likerow.SubjectId})
			if err != nil {
				log.Printf("ERROR retrieving banger post %d: %+v\n", likerow.SubjectId, err)
				return nil
			}
			if len(posts) != 1 {
				return nil
			}

			post := posts[0]
			now := d.clock.NowUnix()
			neg := true
			ver := int64(1)
			label := &atproto.LabelDefs_Label{
				Cts: time.Unix(now, 0).UTC().Format(time.RFC3339),
				Src: LabelerDid,
				Uri: utils.HydrateUri(post.DehydratedUri, "app.bsky.feed.post"),
				Val: "banger",
				Neg: &neg,
				Ver: &ver,
			}

			sigBuf := new(bytes.Buffer)
			err = label.MarshalCBOR(sigBuf)
			if err != nil {
				return err
			}

			sigBytes, err := d.SigningKey.HashAndSign(sigBuf.Bytes())
			if err != nil {
				return err
			}
			label.Sig = sigBytes

			cborBuf := new(bytes.Buffer)
			err = label.MarshalCBOR(cborBuf)
			if err != nil {
				return err
			}

			row := &CustomLabel{
				SubjectType: PostLabelType,
				SubjectId:   likerow.SubjectId,
				CreatedAt:   now,
				LabelId:     banger.LabelId,
				Neg:         1,
				Cbor:        cborBuf.Bytes(),
			}

			return d.CustomLabels.InsertLabels([]*CustomLabel{row})
		},
	)
	if len(errs) > 0 {
		msg := fmt.Sprintf("Error deleting like %s:", uri)
		for _, e := range errs {
			msg = fmt.Sprintf("%s\n%s", msg, e)
		}
		log.Print(msg)
		return errors.New(msg)
	}

	return nil
}

func (d *DBx) DeleteRepost(uri string) error {
	if !d.extendedIndexing {
		return nil
	}

	repostrow := &RepostRow{}
	err := d.Reposts.Get(repostrow, "SELECT * FROM reposts WHERE uri = ?", utils.DehydrateUri(uri))
	if errors.Is(err, sql.ErrNoRows) || (repostrow.RepostId == 0) {
		return nil
	} else if err != nil {
		return err
	}

	errs := ParallelizeFuncsWithRetries(
		func() error {
			_, err := d.Reposts.Exec("DELETE FROM reposts WHERE repost_id = $1", repostrow.RepostId)
			return err
		},
		func() error {
			_, err := d.Posts.Exec("UPDATE posts SET reposts = reposts - 1 WHERE post_id = $1", repostrow.SubjectId)
			return err
		},
	)
	if len(errs) > 0 {
		msg := fmt.Sprintf("Error deleting repost %s:", uri)
		for _, e := range errs {
			msg = fmt.Sprintf("%s\n%s", msg, e)
		}
		log.Print(msg)
		return errors.New(msg)
	}

	return nil
}

func (d *DBx) InsertFollow(followRef *firehose.FollowRef) error {
	/*
		clock := d.clock
		startMethod := clock.NowUnixMilli()
		metric := metrics.NewInsertFollow()
		defer func() {
			metric.Val = clock.NowUnixMilli() - startMethod
			Collector.InsertFollow(metric)
		}()
	*/

	uri := followRef.Ref.Uri
	deferredActorId := NewDeferredInt64()
	deferredSubjectId := NewDeferredInt64()
	deferredFollowId := NewDeferredInt64()

	errs := ParallelizeFuncsWithRetries(
		func() error {
			defer func() {
				//metric.FindActors = clock.NowUnixMilli() - startMethod
				deferredActorId.Cancel()
				deferredSubjectId.Cancel()
			}()

			author := utils.ParseDid(uri)
			subject := followRef.Subject
			if author == subject {
				log.Printf("ignoring following self: %#v\n", followRef)
				return nil
			}

			actors, err := d.Actors.FindOrCreateActors([]string{author, subject})
			if err != nil {
				return err
			}

			for _, actor := range actors {
				did := actor.Did
				id := actor.ActorId
				if author == did {
					deferredActorId.Done(id)
				}
				if subject == did {
					deferredSubjectId.Done(id)
				}
			}

			return nil
		},
		func() error {
			defer deferredFollowId.Cancel()

			actorid := deferredActorId.Get()
			subjectid := deferredSubjectId.Get()
			if (actorid == 0) || (subjectid == 0) {
				return nil
			}

			//defer func() { metric.Insert = clock.NowUnixMilli() - startMethod }()

			followrow := &FollowRow{
				ActorId:   actorid,
				Rkey:      utils.ParseRkey(uri),
				SubjectId: subjectid,
				CreatedAt: d.clock.NowUnix(),
			}

			_, err := d.Follows.InsertFollow(followrow)
			if err != nil {
				return err
			}

			deferredFollowId.Done(followrow.FollowId)

			return err
		},
		func() error {
			//defer func() { metric.Index.Val = clock.NowUnixMilli() - startMethod }()

			actorid := deferredActorId.Get()
			if actorid == 0 {
				return nil
			}

			//startBranch := clock.NowUnixMilli()

			indexed, err := d.FollowsIndexed.FindByActorId(actorid)
			//metric.Index.FindIndex = clock.NowUnixMilli() - startBranch
			if err != nil {
				return err
			}
			if (indexed == nil) || (indexed.LastFollow < 0) {
				return nil
			}

			followid := deferredFollowId.Get()
			if followid == 0 {
				return nil
			}

			//startSetLastFollow := clock.NowUnixMilli()
			err = d.FollowsIndexed.SetLastFollow(actorid, followid)
			//metric.Index.SetLastFollow = clock.NowUnixMilli() - startSetLastFollow
			if err != nil {
				return err
			}

			return nil
		},
	)
	if len(errs) > 0 {
		msg := fmt.Sprintf("Error indexing follow %s:", uri)
		for _, e := range errs {
			msg = fmt.Sprintf("%s\n%s", msg, e)
		}
		log.Print(msg)
		return errors.New(msg)
	}

	return nil
}
func (d *DBx) DeleteFollow(uri string) error {
	/*
		clock := d.clock
		startMethod := clock.NowUnixMilli()
		metric := metrics.NewDeleteFollow()
		defer func() {
			metric.Val = clock.NowUnixMilli() - startMethod
			Collector.DeleteFollow(metric)
		}()
	*/

	did := utils.ParseDid(uri)
	if did == "" {
		return nil
	}

	rkey := utils.ParseRkey(uri)
	if rkey == "" {
		return nil
	}

	actorrow, err := d.Actors.FindOrCreateActor(did)
	if err != nil {
		return err
	}

	_, err = d.Follows.Exec("DELETE FROM follows WHERE actor_id = ? AND rkey = ?", actorrow.ActorId, rkey)
	if (err != nil) && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	return nil
}

type queryable interface {
	Get(dest interface{}, query string, args ...interface{}) error
	Exec(query string, args ...any) (sql.Result, error)
	NamedExec(query string, arg interface{}) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	QueryRowx(query string, args ...interface{}) *sqlx.Row
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)
	Select(dest interface{}, query string, args ...interface{}) error
}

func queryHasResults(d queryable, q string, args ...interface{}) (bool, error) {
	hit := 0
	row := d.QueryRowx(fmt.Sprintf("SELECT 1 FROM (%s LIMIT 1)", q), args...)
	err := row.Scan(&hit)
	if (err != nil) && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	} else if err != nil {
		return false, nil
	}
	return hit != 0, nil
}

func (d *DBx) Prune(since int64, limit int) (int, error) {
	postrows := make([]*PostRow, 0, limit)
	err := d.Posts.Select(&postrows, "SELECT post_id FROM posts WHERE created_at < $1 ORDER BY created_at ASC, post_id ASC LIMIT $2", since, limit)
	if errors.Is(err, sql.ErrNoRows) || (len(postrows) == 0) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	cutoff := postrows[len(postrows)-1].PostId
	errs := ParallelizeFuncsWithRetries(
		func() error {
			exists, err := queryHasResults(d.Likes, "SELECT 1 FROM likes WHERE subject_id > 0 AND subject_id <= $1", cutoff)
			if err != nil {
				return err
			} else if !exists {
				return nil
			}

			_, err = d.Likes.Exec("DELETE FROM likes WHERE subject_id > 0 AND subject_id <= $1", cutoff)
			return err
		},
		func() error {
			exists, err := queryHasResults(d.Reposts, "SELECT 1 FROM reposts WHERE subject_id > 0 AND subject_id <= $1", cutoff)
			if err != nil {
				return err
			} else if !exists {
				return nil
			}

			_, err = d.Reposts.Exec("DELETE FROM reposts WHERE subject_id > 0 AND subject_id <= $1", cutoff)
			return err
		},
		func() error {
			exists, err := queryHasResults(d.Mentions, "SELECT 1 FROM mentions WHERE post_id > 0 AND post_id <= $1", cutoff)
			if err != nil {
				return err
			} else if !exists {
				return nil
			}

			_, err = d.Mentions.Exec("DELETE FROM mentions WHERE post_id > 0 AND post_id <= $1", cutoff)
			if err != nil {
				return err
			}
			return nil
		},
		/*
			func() error {
				exists, err := queryHasResults(d.ThreadMentions, "SELECT 1 FROM thread_mentions WHERE post_id > 0 AND post_id <= $1", cutoff)
				if err != nil {
					return err
				} else if !exists {
					return nil
				}

				_, err = d.ThreadMentions.Exec("DELETE FROM thread_mentions WHERE post_id > 0 AND post_id <= $1", cutoff)
				if err != nil {
					return err
				}
				return nil
			},
		*/
		func() error {
			exists, err := queryHasResults(d.Quotes, "SELECT 1 FROM quotes WHERE post_id > 0 AND post_id <= $1", cutoff)
			if err != nil {
				return err
			} else if !exists {
				return nil
			}

			_, err = d.Quotes.Exec("DELETE FROM quotes WHERE post_id > 0 AND post_id <= $1", cutoff)
			if err != nil {
				return err
			}

			return nil
		},
		func() error {
			exists, err := queryHasResults(d.Replies, "SELECT 1 FROM replies WHERE post_id > 0 AND post_id <= $1", cutoff)
			if err != nil {
				return err
			} else if !exists {
				return nil
			}

			_, err = d.Replies.Exec("DELETE FROM replies WHERE post_id > 0 AND post_id <= $1", cutoff)
			if err != nil {
				return err
			}

			return nil
		},
		func() error {
			exists, err := queryHasResults(d.PostLabels, "SELECT 1 FROM post_labels WHERE post_id > 0 AND post_id <= $1", cutoff)
			if err != nil {
				return err
			} else if !exists {
				return nil
			}

			_, err = d.PostLabels.Exec("DELETE FROM post_labels WHERE post_id > 0 AND post_id <= $1", cutoff)
			if err != nil {
				return err
			}
			return nil
		},
		/*
			func() error {
				exists, err := queryHasResults(d.Dms, "SELECT 1 FROM dms WHERE post_id > 0 AND post_id <= $1", cutoff)
				if err != nil {
					return err
				} else if !exists {
					return nil
				}

				_, err = d.Dms.Exec("DELETE FROM dms WHERE post_id > 0 AND post_id <= $1", cutoff)
				if err != nil {
					return err
				}
				return nil
			},
		*/
	)
	if len(errs) > 0 {
		msg := fmt.Sprintf("Error pruning posts from cutoff %d:", cutoff)
		for _, e := range errs {
			msg = fmt.Sprintf("%s\n%s", msg, e)
		}
		log.Print(msg)
		return 0, errors.New(msg)
	}

	_, err = d.Posts.Exec("DELETE FROM posts WHERE post_id > 0 AND post_id <= $1", cutoff)
	if err != nil {
		return 0, err
	}

	return len(postrows), nil
}

func (d *DBx) selectMentions(before int64, limit int, actorid int64) ([]*PostRow, bool, error) {
	mentions := make([]int64, 0, limit)
	quotes := make([]int64, 0, limit)
	replies := make([]int64, 0, limit)

	errs := ParallelizeFuncsWithRetries(
		func() error {
			rows, err := d.Mentions.SelectMentionsActorId(actorid, before, limit)
			if err != nil {
				return nil
			}

			mentions = rows
			return nil
		},
		func() error {
			rows, err := d.Quotes.SelectQuotesByActorId(actorid, before, limit)
			if err != nil {
				return nil
			}

			quotes = rows
			return nil
		},
		func() error {
			rows, err := d.Replies.SelectRepliesToActorId(actorid, before, limit)
			if err != nil {
				return nil
			}

			replies = rows
			return nil
		},
	)
	if len(errs) > 0 {
		msg := fmt.Sprintf("Error selecting mentions for actor id %d:", actorid)
		for _, e := range errs {
			msg = fmt.Sprintf("%s\n%s", msg, e)
		}
		log.Print(msg)
		return nil, false, errors.New(msg)
	}

	postids := uniqueInt64s(concatInt64s(mentions, quotes, replies))
	posts, err := d.Posts.SelectPostsById(postids)
	if err != nil {
		return nil, false, err
	}

	sortByPostIdDesc(posts)

	if len(posts) <= limit {
		return posts, false, nil
	}

	return posts[:limit], true, nil
}

func (d *DBx) SelectMentions(before int64, limit int, did string) ([]*PostRow, error) {
	actor, err := d.Actors.FindOrCreateActor(did)
	if err != nil {
		return nil, err
	} else if (actor == nil) || (actor.ActorId == 0) {
		fmt.Printf("no actor found for %s\n", did)
		return nil, nil
	}

	actorid := actor.ActorId
	mentions := make([]*PostRow, 0, limit)
	iterBefore := before
	iterLimit := limit
	for {
		posts, more, err := d.selectMentions(iterBefore, iterLimit, actorid)
		if err != nil {
			return nil, err
		}

		for _, post := range posts {
			mentions = append(mentions, post)
		}

		if !more {
			break
		}

		iterLimit = limit - len(mentions)
		if iterLimit <= 0 {
			break
		}

		iterBefore = posts[len(posts)-1].PostId
	}

	return mentions, nil
}

func (d *DBx) SelectQuotesForUri(uri string) ([]*PostRow, error) {
	post, err := d.Posts.FindByUri(uri)
	if err != nil {
		return nil, err
	}
	if (post == nil) || (post.PostId == 0) {
		return nil, nil
	}

	last := SQLiteMaxInt
	quotes := make([]int64, 0)
	chunk := 100
	for {
		ids, err := d.Quotes.SelectQuotesBySubjectId(post.PostId, last, chunk)
		if err != nil {
			return nil, err
		}
		if ids == nil {
			break
		}

		quotes = append(quotes, ids...)
		if len(quotes) < chunk {
			break
		}

		last = quotes[len(quotes)-1]
	}

	return d.Posts.SelectPostsById(quotes)
}

func (d *DBx) SelectQuotes(before int64, limit int, did string) ([]*PostRow, error) {
	actor, err := d.Actors.FindOrCreateActor(did)
	if err != nil {
		return nil, err
	} else if (actor == nil) || (actor.ActorId == 0) {
		fmt.Printf("no actor found for %s\n", did)
		return nil, nil
	}

	actorid := actor.ActorId

	postids, err := d.Quotes.SelectQuotesByActorId(actorid, before, limit)
	if err != nil {
		return nil, err
	}

	posts, err := d.Posts.SelectPostsById(uniqueInt64s(postids))
	if err != nil {
		return nil, err
	}

	sortByPostIdDesc(posts)

	if len(posts) <= limit {
		return posts, nil
	}

	return posts[:limit], nil
}

func (d *DBx) selectFollows(actorid int64) ([]int64, bool, error) {
	f := d.Follows

	last, err := d.FollowsIndexed.FindOrCreateByActorId(actorid)
	if err != nil {
		return nil, false, err
	}

	indexed := last.LastFollow >= 0
	cached, ok := f.cache.Get(actorid)
	if ok {
		if last.LastFollow > cached.last {
			rows, err := f.SelectFollows(actorid, cached.last, 100)
			if err != nil {
				return nil, false, err
			}

			for _, row := range rows {
				cached.follows = append(cached.follows, row.SubjectId)
			}

			cached.last = rows[len(rows)-1].FollowId
		}

		return cached.follows, indexed, nil
	}

	var done bool = false
	var lastid int64 = 0
	follows := make([]int64, 0)
	for !done {
		rows, err := f.SelectFollows(actorid, lastid, 100)
		if err != nil {
			return nil, false, err
		}
		if (rows == nil) || (len(rows) == 0) {
			done = true
			break
		}

		for _, row := range rows {
			follows = append(follows, row.SubjectId)
		}
		lastid = rows[len(rows)-1].FollowId
	}

	f.cache.Add(lastid, &followCacheEntry{follows: follows, last: lastid})
	return follows, indexed, nil
}

func (d *DBx) selectActorsWithBirthdays(actorids []int64) ([]int64, error) {
	if (actorids == nil) || (len(actorids) == 0) {
		return []int64{}, nil
	}

	birthdayboys := make([]int64, 0, len(actorids))
	birthday, err := d.Labels.FindOrCreateLabel("birthday")
	if err != nil {
		return nil, err
	}

	actoridStrings := make([]string, len(actorids))
	for i, actorid := range actorids {
		actoridStrings[i] = fmt.Sprintf("%d", actorid)
	}

	q := fmt.Sprintf(
		`
	  SELECT
	    subject_id
	  FROM
		custom_labels
	  WHERE
		neg = 0
		AND label_id = $1
		AND subject_id IN (%s)
		AND subject_type = $2
	  ORDER BY
		custom_label_id DESC
		`,
		strings.Join(actoridStrings, ","),
	)

	err = d.CustomLabels.DB.Select(&birthdayboys, q, birthday.LabelId, AccountLabelType)
	if err != nil {
		return nil, err
	}

	return birthdayboys, nil
}

func (d *DBx) selectAllBirthdays() ([]int64, error) {
	label, err := d.Labels.FindOrCreateLabel("birthday")
	if err != nil {
		return nil, err
	}

	chunk := 100
	var since int64 = 0
	birthdayboys := make([]int64, 0)
	for {
		birthdays, err := d.CustomLabels.SelectLabelsByLabelIdAndNeg(label.LabelId, false, since, chunk)
		if err != nil {
			return nil, err
		}
		if birthdays == nil {
			return birthdayboys, nil
		}

		for _, birthday := range birthdays {
			if birthday.SubjectType != AccountLabelType {
				continue
			}

			birthdayboys = append(birthdayboys, birthday.SubjectId)
		}

		if len(birthdays) < chunk {
			return birthdayboys, nil
		}

		since = birthdays[len(birthdays)-1].CustomLabelId
	}
}

func (d *DBx) SelectBangers(before int64, limit int) ([]*PostRow, error) {
	if before == 0 {
		before = SQLiteMaxInt
	}

	banger, err := d.Labels.FindOrCreateLabel("banger")
	if err != nil {
		return nil, err
	}

	labels := make([]*CustomLabel, 0, limit)
	err = d.CustomLabels.Select(
		&labels,
		"SELECT * FROM custom_labels WHERE label_id = $1 AND neg = 0 AND subject_type = $2 AND custom_label_id < $3 ORDER BY custom_label_id DESC LIMIT $4",
		banger.LabelId,
		PostLabelType,
		before,
		limit,
	)
	if err != nil {
		return nil, err
	}

	postIds := make([]int64, 0, len(labels))
	for _, label := range labels {
		postIds = append(postIds, label.SubjectId)
	}

	posts, err := d.Posts.SelectPostsById(postIds)
	if err != nil {
		return nil, err
	}

	postIdToPost := make(map[int64]*PostRow)
	for _, post := range posts {
		postIdToPost[post.PostId] = post
	}

	sortedPosts := make([]*PostRow, 0, len(posts))
	for _, label := range labels {
		post := postIdToPost[label.SubjectId]
		if post == nil {
			continue
		}
		// set postid to customlabelid to use customlabelid as cursor
		post.PostId = label.CustomLabelId
		sortedPosts = append(sortedPosts, post)
	}

	return sortedPosts, nil
}

func (d *DBx) SelectBirthdays(before int64, limit int) ([]*PostRow, error) {
	birthdayboys, err := d.selectAllBirthdays()
	if err != nil {
		return nil, err
	}

	birthdays := make([]*PostRow, 0, limit)

	done := false
	for !done {
		posts, err := d.Posts.SelectPostsByActorIds(birthdayboys, before, limit)
		if err != nil {
			return nil, err
		}

		for _, post := range posts {
			birthdays = append(birthdays, post)
		}

		if (len(posts) < limit) || (len(birthdays) >= limit) {
			done = true
			break
		}

		before = posts[len(posts)-1].PostId
	}

	return birthdays, nil
}

func (d *DBx) SelectBirthdaysFollowed(before int64, limit int, did string) ([]*PostRow, error) {
	actor, err := d.Actors.FindOrCreateActor(did)
	if err != nil {
		return nil, err
	}
	if actor.Blocked {
		return nil, nil
	}
	actorid := actor.ActorId
	if actorid == 0 {
		return nil, nil
	}

	follows, indexed, err := d.selectFollows(actorid)
	if err != nil {
		return nil, err
	}

	birthdayboys, err := d.selectActorsWithBirthdays(follows)
	if err != nil {
		return nil, err
	}

	birthdays := make([]*PostRow, 0, limit)
	if (!indexed) && (PinnedFollowPost != nil) {
		return []*PostRow{PinnedFollowPost}, nil
	} else if (birthdayboys == nil) || (len(birthdayboys) == 0) {
		return []*PostRow{}, nil
	}

	var isbirthdayboy map[int64]bool = nil

	done := false
	for !done {
		posts, err := d.Posts.SelectPostsByActorIds(birthdayboys, before, limit)
		if err != nil {
			return nil, err
		}

		if isbirthdayboy == nil {
			isbirthdayboy = make(map[int64]bool)
			for _, birthdayboy := range birthdayboys {
				isbirthdayboy[birthdayboy] = true
			}
		}

		for _, post := range posts {
			if isbirthdayboy[post.ActorId] {
				birthdays = append(birthdays, post)
			}
		}

		if (len(posts) < limit) || (len(birthdays) >= limit) {
			done = true
			break
		}

		before = posts[len(posts)-1].PostId
	}

	return birthdays, nil
}

func (d *DBx) SelectAllMentionsFollowed(before int64, limit int, did string) ([]*PostRow, error) {
	deferredFollows := NewDeferredInt64s()

	actor, err := d.Actors.FindOrCreateActor(did)
	if err != nil {
		return nil, err
	}
	if actor.Blocked {
		return nil, nil
	}
	actorid := actor.ActorId

	isIndexed := false
	mentions := make([]*PostRow, 0, limit)
	errs := ParallelizeFuncsWithRetries(
		func() error {
			defer deferredFollows.Cancel()

			if actorid == 0 {
				return nil
			}

			follows, indexed, err := d.selectFollows(actorid)
			if err != nil {
				return err
			}

			isIndexed = indexed
			deferredFollows.Done(follows)

			return nil
		},
		func() error {
			var isFollow map[int64]bool = nil

			done := false
			for !done {
				posts, err := d.SelectAllMentions(before, limit, did)
				if err != nil {
					return err
				}

				if isFollow == nil {
					follows := deferredFollows.Get()
					if (follows == nil) || (len(follows) == 0) {
						return nil
					}

					isFollow = make(map[int64]bool)
					for _, follow := range follows {
						isFollow[follow] = true
					}
				}

				for _, post := range posts {
					if isFollow[post.ActorId] {
						mentions = append(mentions, post)
					}
				}

				if (len(posts) < limit) || (len(mentions) >= limit) {
					done = true
					break
				}

				before = posts[len(posts)-1].PostId
			}

			return nil
		},
	)
	if len(errs) > 0 {
		msg := fmt.Sprintf("Error selecting followed mentions for %s:", did)
		for _, e := range errs {
			msg = fmt.Sprintf("%s\n%s", msg, e)
		}
		log.Print(msg)
		return nil, errors.New(msg)
	}

	sortByPostIdDesc(mentions)
	if !isIndexed && (PinnedFollowPost != nil) {
		mentions = append([]*PostRow{PinnedFollowPost}, mentions...)
	}

	if len(mentions) <= limit {
		return mentions, nil
	}

	return mentions[:limit], nil
}

func (d *DBx) SelectMentionsFollowed(before int64, limit int, did string) ([]*PostRow, error) {
	deferredFollows := NewDeferredInt64s()

	actor, err := d.Actors.FindOrCreateActor(did)
	if err != nil {
		return nil, err
	}
	if actor.Blocked {
		return nil, nil
	}
	actorid := actor.ActorId

	isIndexed := false
	mentions := make([]*PostRow, 0, limit)
	errs := ParallelizeFuncsWithRetries(
		func() error {
			defer deferredFollows.Cancel()

			if actorid == 0 {
				return nil
			}

			follows, indexed, err := d.selectFollows(actorid)
			if err != nil {
				return err
			}

			isIndexed = indexed
			deferredFollows.Done(follows)

			return nil
		},
		func() error {
			var isFollow map[int64]bool = nil

			done := false
			for !done {
				posts, err := d.SelectMentions(before, limit, did)
				if err != nil {
					return err
				}

				if isFollow == nil {
					follows := deferredFollows.Get()
					if (follows == nil) || (len(follows) == 0) {
						return nil
					}

					isFollow = make(map[int64]bool)
					for _, follow := range follows {
						isFollow[follow] = true
					}
				}

				for _, post := range posts {
					if isFollow[post.ActorId] {
						mentions = append(mentions, post)
					}
				}

				if (len(posts) < limit) || (len(mentions) >= limit) {
					done = true
					break
				}

				before = posts[len(posts)-1].PostId
			}

			return nil
		},
	)
	if len(errs) > 0 {
		msg := fmt.Sprintf("Error selecting followed mentions for %s:", did)
		for _, e := range errs {
			msg = fmt.Sprintf("%s\n%s", msg, e)
		}
		log.Print(msg)
		return nil, errors.New(msg)
	}

	sortByPostIdDesc(mentions)
	if !isIndexed && (PinnedFollowPost != nil) {
		mentions = append([]*PostRow{PinnedFollowPost}, mentions...)
	}

	if len(mentions) <= limit {
		return mentions, nil
	}

	return mentions[:limit], nil
}

func (d *DBx) SelectAllMentions(before int64, limit int, did string) ([]*PostRow, error) {
	actor, err := d.Actors.FindOrCreateActor(did)
	if err != nil {
		return nil, err
	} else if (actor == nil) || (actor.ActorId == 0) {
		fmt.Printf("no actor found for %s\n", did)
		return nil, nil
	}

	postIds, err := d.ThreadMentions.SelectThreadMentionsByActorId(actor.ActorId, before, limit)
	if err != nil {
		return nil, err
	}

	posts, err := d.Posts.SelectPostsById(postIds)
	if err != nil {
		return nil, err
	}

	return sortByPostIdDesc(posts), nil
}

func (d *DBx) SelectDms(before int64, limit int, did string) ([]*PostRow, error) {
	actor, err := d.Actors.FindOrCreateActor(did)
	if err != nil {
		return nil, err
	} else if (actor == nil) || (actor.ActorId == 0) {
		fmt.Printf("no actor found for %s\n", did)
		return nil, nil
	}

	postIds, err := d.Dms.SelectDmsByActorId(actor.ActorId, before, limit)
	if err != nil {
		return nil, err
	}

	posts, err := d.Posts.SelectPostsById(postIds)
	if err != nil {
		return nil, err
	}

	return sortByPostIdDesc(posts), nil
}

func (d *DBx) SelectPostsByLabels(before int64, limit int, labelNames ...string) ([]*PostRow, error) {
	var err error = nil
	if before == SQLiteMaxInt {
		before, err = d.Posts.SelectPostIdByEpoch(d.clock.NowUnix() - (5 * 60))
		if err != nil {
			return nil, err
		}
	}

	postids := make([]int64, 0, limit*len(labelNames))
	for _, labelName := range labelNames {
		label, err := d.Labels.FindLabel(labelName)
		if err != nil {
			return nil, err
		} else if label == nil {
			continue
		}

		ps, err := d.PostLabels.SelectPostsByLabel(label.LabelId, before, limit)
		if err != nil {
			return nil, err
		}
		for _, p := range ps {
			postids = append(postids, p)
		}
	}

	posts, err := d.Posts.SelectPostsById(uniqueInt64s(postids))
	if err != nil {
		return nil, err
	}

	sortByPostIdDesc(posts)

	if len(posts) <= limit {
		return posts, nil
	}

	return posts[:limit], nil
}

func (d *DBx) SelectPostsByLabelsFollowed(before int64, limit int, did string, labelNames ...string) ([]*PostRow, error) {
	deferredFollows := NewDeferredInt64s()

	actor, err := d.Actors.FindOrCreateActor(did)
	if err != nil {
		return nil, err
	}
	if actor.Blocked {
		return nil, nil
	}
	actorid := actor.ActorId

	isIndexed := false
	results := make([]*PostRow, 0, limit*len(labelNames))
	errs := ParallelizeFuncsWithRetries(
		func() error {
			defer deferredFollows.Cancel()

			if actorid == 0 {
				return nil
			}

			follows, indexed, err := d.selectFollows(actorid)
			if err != nil {
				return err
			}

			isIndexed = indexed
			deferredFollows.Done(follows)

			return nil
		},
		func() error {
			var isFollow map[int64]bool = nil

			done := false
			for !done {
				posts, err := d.SelectPostsByLabels(before, limit, labelNames...)
				if err != nil {
					return err
				}

				if isFollow == nil {
					follows := deferredFollows.Get()
					if (follows == nil) || (len(follows) == 0) {
						return nil
					}

					isFollow = make(map[int64]bool)
					for _, follow := range follows {
						isFollow[follow] = true
					}
				}

				for _, post := range posts {
					if isFollow[post.ActorId] {
						results = append(results, post)
					}
				}

				if (len(posts) < limit) || (len(results) >= limit) {
					done = true
					break
				}

				before = posts[len(posts)-1].PostId
			}

			return nil
		},
	)
	if len(errs) > 0 {
		msg := fmt.Sprintf("Error selecting followed labeled posts (%s) for %s:", strings.Join(labelNames, ", "), did)
		for _, e := range errs {
			msg = fmt.Sprintf("%s\n%s", msg, e)
		}
		log.Print(msg)
		return nil, errors.New(msg)
	}

	sortByPostIdDesc(results)
	if !isIndexed && (PinnedFollowPost != nil) {
		results = append([]*PostRow{PinnedFollowPost}, results...)
	}

	if len(results) <= limit {
		return results, nil
	}

	return results[:limit], nil
}

func (d *DBx) SelectLatestPosts(before int64, limit int) ([]*PostRow, error) {
	posts := make([]*PostRow, 0, limit)
	err := d.Posts.Select(&posts, "SELECT * FROM posts WHERE post_id < ? ORDER BY post_id DESC LIMIT ?", before, limit)
	if err != nil {
		return nil, err
	}
	for _, post := range posts {
		post.Uri = utils.HydrateUri(post.DehydratedUri, "app.bsky.feed.post")
	}
	return posts, nil
}

func (d *DBx) SelectOnlyPosts(before int64, limit int, dids []string) ([]*PostRow, error) {
	actors, err := d.Actors.FindOrCreateActors(dids)
	if err != nil {
		return nil, err
	}

	actorids := make([]int64, len(actors))
	for i, actor := range actors {
		actorids[i] = actor.ActorId
	}

	return d.selectOnlyPosts(actorids, before, limit)
}

func (d *DBx) selectOnlyPosts(actorids []int64, before int64, limit int) ([]*PostRow, error) {
	isReply := make(map[int64]bool)
	onlyPosts := make([]*PostRow, 0)
	posts := make([]*PostRow, 0)

	if before == 0 {
		before = SQLiteMaxInt
	}
	var lastPost int64 = before
	var lastReply int64 = before

	for {
		errs := ParallelizeFuncsWithRetries(
			func() error {
				var err error = nil
				posts, err = d.Posts.SelectPostsByActorIds(actorids, lastPost, limit)
				return err
			},
			func() error {
				if (lastPost - int64(limit)) >= lastReply {
					return nil
				}

				replies, err := d.Replies.SelectRepliesFromActorIds(actorids, lastPost, limit)
				if err != nil {
					return err
				}
				if (replies == nil) || (len(replies) == 0) {
					return nil
				}

				lastReply = replies[len(replies)-1]
				for _, reply := range replies {
					isReply[reply] = true
				}

				return nil
			},
		)
		if len(errs) > 0 {
			msg := fmt.Sprintf("Error selecting only posts for actor ids %+v:", actorids)
			for _, e := range errs {
				msg = fmt.Sprintf("%s\n%s", msg, e)
			}
			log.Print(msg)
			return nil, errors.New(msg)
		}
		if len(posts) == 0 {
			return onlyPosts, nil
		}

		for _, post := range posts {
			if isReply[post.PostId] {
				continue
			}
			onlyPosts = append(onlyPosts, post)
		}

		if len(onlyPosts) >= limit {
			return onlyPosts[:limit], nil
		}

		lastPost = posts[len(posts)-1].PostId
	}
}

func (d *DBx) selectMark(actorid int64, markid int64, before int64, limit int) ([]*PostRow, bool, error) {
	toplevel := make([]int64, 0, limit)
	mentions := make([]int64, 0, limit)
	quotes := make([]int64, 0, limit)
	replies := make([]int64, 0, limit)

	errs := ParallelizeFuncsWithRetries(
		func() error {
			posts, err := d.selectOnlyPosts([]int64{markid}, before, limit)
			if err != nil {
				return err
			}

			for _, post := range posts {
				toplevel = append(toplevel, post.PostId)
			}

			return nil
		},
		func() error {
			err := d.Mentions.Select(&mentions, "SELECT post_id FROM mentions WHERE actor_id = $1 AND subject_id = $2 AND post_id < $3 ORDER BY post_id DESC LIMIT $4", markid, actorid, before, limit)
			if err != nil {
				return err
			}
			return nil
		},
		func() error {
			err := d.Quotes.Select(&quotes, "SELECT post_id FROM quotes WHERE actor_id = $1 AND subject_actor_id = $2 AND post_id < $3 ORDER BY post_id DESC LIMIT $4", markid, actorid, before, limit)
			if err != nil {
				return err
			}
			return nil
		},
		func() error {
			err := d.Replies.Select(&replies, "SELECT post_id FROM replies WHERE actor_id = $1 AND parent_actor_id = $2 AND post_id < $3 ORDER BY post_id DESC LIMIT $4", markid, actorid, before, limit)
			if err != nil {
				return err
			}
			return nil
		},
	)
	if len(errs) > 0 {
		msg := fmt.Sprintf("Error selecting mark mentions for actor id %d:", actorid)
		for _, e := range errs {
			msg = fmt.Sprintf("%s\n%s", msg, e)
		}
		log.Print(msg)
		return nil, false, errors.New(msg)
	}

	postids := uniqueInt64s(concatInt64s(toplevel, mentions, quotes, replies))
	posts, err := d.Posts.SelectPostsById(postids)
	if err != nil {
		return nil, false, err
	}

	sortByPostIdDesc(posts)

	if len(posts) <= limit {
		return posts, false, nil
	}

	return posts[:limit], true, nil
}

func (d *DBx) SelectMark(before int64, limit int, did string) ([]*PostRow, error) {
	mark, err := d.Actors.FindOrCreateActor(MARK)
	if err != nil {
		return nil, err
	} else if mark == nil {
		fmt.Printf("no actor found for mark\n")
		return nil, nil
	}

	actor, err := d.Actors.FindOrCreateActor(did)
	if err != nil {
		return nil, err
	} else if actor == nil {
		fmt.Printf("no actor found for %s\n", did)
		return nil, nil
	}

	actorid := actor.ActorId
	mentions := make([]*PostRow, 0, limit)
	iterBefore := before
	iterLimit := limit
	for {
		posts, more, err := d.selectMark(actorid, mark.ActorId, iterBefore, iterLimit)
		if err != nil {
			return nil, err
		}

		for _, post := range posts {
			mentions = append(mentions, post)
		}

		if !more {
			break
		}

		iterLimit = limit - len(mentions)
		if iterLimit <= 0 {
			break
		}

		iterBefore = posts[len(posts)-1].PostId
	}

	return mentions, nil
}

func (d *DBx) SelectPostLabels(since int64, limit int) ([]*LabelDef, error) {

	excludedLabels := []string{"ceusemlimites", "first20", "gmgn", "newskie", "rembangs", "renewskie"}
	excludedLabelPlcs := make([]string, 0, len(excludedLabels))
	queryParams := make([]any, 0, len(excludedLabels)+1)
	for _, name := range excludedLabels {
		label, err := d.Labels.FindOrCreateLabel(name)
		if err != nil {
			return nil, err
		}
		queryParams = append(queryParams, label.LabelId)
		excludedLabelPlcs = append(excludedLabelPlcs, "?")
	}

	var err error
	postlabels := make([]*PostLabelRow, 0, limit)
	q := fmt.Sprintf("SELECT * FROM post_labels WHERE label_id NOT IN (%s)", strings.Join(excludedLabelPlcs, ","))
	if since == 0 {
		queryParams = append(queryParams, limit)
		err = d.PostLabels.Select(&postlabels, fmt.Sprintf("SELECT * FROM (%s ORDER BY post_label_id DESC LIMIT ?) ORDER BY post_label_id ASC", q), queryParams...)
	} else {
		queryParams = append(queryParams, since, limit)
		err = d.PostLabels.Select(&postlabels, fmt.Sprintf("%s AND post_label_id > ? ORDER BY post_label_id ASC LIMIT ?", q), queryParams...)
	}
	if err != nil {
		return nil, err
	}

	postids := make([]int64, len(postlabels))
	for i, postlabel := range postlabels {
		postids[i] = postlabel.PostId
	}

	posts, err := d.Posts.SelectPostsById(postids)
	if err != nil {
		return nil, err
	}

	postsById := make(map[int64]*PostRow)
	for _, post := range posts {
		postsById[post.PostId] = post
	}

	labelDefs := make([]*LabelDef, 0, limit)
	for _, postlabel := range postlabels {
		post, ok := postsById[postlabel.PostId]
		if !ok {
			continue
		}

		label, err := d.Labels.FindLabelByLabelId(postlabel.LabelId)
		if err != nil {
			return nil, err
		}

		labelDefs = append(labelDefs, &LabelDef{
			PostLabelId: postlabel.PostLabelId,
			CreatedAt:   post.CreatedAt,
			Uri:         post.Uri,
			Val:         label.Name,
		})
	}

	return labelDefs, nil
}
func (d *DBx) RecordBirthdayLabels(t clock.Clock) error {
	now := time.Unix(t.NowUnix(), 0)
	endWindow := now.AddDate(-1, 0, 0)
	startWindow := endWindow.Add(-10 * time.Minute)

	actors, err := d.Actors.SelectActorsWithirthdaysBetween(startWindow.Unix(), endWindow.Unix())
	if err != nil {
		return err
	}

	bday, err := d.Labels.FindOrCreateLabel("birthday")
	if err != nil {
		return err
	}

	labels := make([]*CustomLabel, len(actors))
	for i, actor := range actors {
		ver := int64(1)
		label := &atproto.LabelDefs_Label{
			Cts: now.UTC().Format(time.RFC3339),
			Src: LabelerDid,
			Uri: actor.Did,
			Val: "birthday",
			Ver: &ver,
		}

		sigBuf := new(bytes.Buffer)
		err := label.MarshalCBOR(sigBuf)
		if err != nil {
			return err
		}

		sigBytes, err := d.SigningKey.HashAndSign(sigBuf.Bytes())
		if err != nil {
			return err
		}
		label.Sig = sigBytes

		cborBuf := new(bytes.Buffer)
		err = label.MarshalCBOR(cborBuf)
		if err != nil {
			return err
		}

		labels[i] = &CustomLabel{
			SubjectType: AccountLabelType,
			SubjectId:   actor.ActorId,
			CreatedAt:   now.Unix(),
			LabelId:     bday.LabelId,
			Neg:         0,
			Cbor:        cborBuf.Bytes(),
		}
	}

	return d.CustomLabels.InsertLabels(labels)
}

func (d *DBx) RecordUnbirthdayLabels(t clock.Clock) error {
	now := time.Unix(t.NowUnix(), 0)
	endWindow := now.AddDate(-1, 0, -1)
	startWindow := endWindow.Add(-10 * time.Minute)

	actors, err := d.Actors.SelectActorsWithirthdaysBetween(startWindow.Unix(), endWindow.Unix())
	if err != nil {
		return err
	}

	actoridStrings := make([]string, len(actors))
	for i, actor := range actors {
		actoridStrings[i] = fmt.Sprintf("%d", actor.ActorId)
	}

	bday, err := d.Labels.FindOrCreateLabel("birthday")
	if err != nil {
		return err
	}

	labels := make([]*CustomLabel, 0, len(actors))
	for _, actor := range actors {
		did := actor.Did
		if (did == REM) || (did == AMELIA_BUT_ALSO_ITS_REM) {
			// it is always rem's birthday
			continue
		}

		neg := true
		ver := int64(1)
		label := &atproto.LabelDefs_Label{
			Cts: now.UTC().Format(time.RFC3339),
			Src: LabelerDid,
			Uri: did,
			Val: "birthday",
			Neg: &neg,
			Ver: &ver,
		}

		sigBuf := new(bytes.Buffer)
		err := label.MarshalCBOR(sigBuf)
		if err != nil {
			return err
		}

		sigBytes, err := d.SigningKey.HashAndSign(sigBuf.Bytes())
		if err != nil {
			return err
		}
		label.Sig = sigBytes

		cborBuf := new(bytes.Buffer)
		err = label.MarshalCBOR(cborBuf)
		if err != nil {
			return err
		}

		labels = append(
			labels,
			&CustomLabel{
				SubjectType: AccountLabelType,
				SubjectId:   actor.ActorId,
				CreatedAt:   now.Unix(),
				LabelId:     bday.LabelId,
				Neg:         1,
				Cbor:        cborBuf.Bytes(),
			})
	}

	err = d.CustomLabels.InsertLabels(labels)
	if err != nil {
		return err
	}

	_, err = d.CustomLabels.Exec(
		fmt.Sprintf(
			`
			DELETE FROM
			custom_labels
		  WHERE
			label_id = $1
			AND neg = 0
			AND subject_type = $2
			AND subject_id IN (%s)
			`,
			strings.Join(actoridStrings, ","),
		),
		bday.LabelId,
		AccountLabelType,
	)

	return err
}

func (d *DBx) PruneCustomLabels(t clock.Clock) error {
	now := time.Unix(t.NowUnix(), 0)
	end := now.AddDate(0, 0, -7)

	_, err := d.CustomLabels.Exec(
		"DELETE FROM custom_labels WHERE created_at < ?",
		end.Unix(),
	)

	return err
}

func (d *DBx) SelectLastCustomLabelId() (int64, error) {
	var lastid int64
	row := d.CustomLabels.QueryRowx("SELECT custom_label_id FROM custom_labels ORDER BY custom_label_id DESC LIMIT 1")
	err := row.Scan(&lastid)
	if err != nil {
		return 0, err
	}
	return lastid, nil
}

func (d *DBx) SelectCustomLabels(before int64, limit int) ([]*CustomLabel, error) {

	customLabels, err := d.CustomLabels.SelectLabels(before, limit)
	if err != nil {
		return nil, err
	}

	return customLabels, nil
}

func (d *DBx) SelectCustomAccountLabels(labelName string, before int64, limit int) ([]*CustomLabel, error) {
	label, err := d.Labels.FindOrCreateLabel(labelName)
	if err != nil {
		return nil, err
	}

	customLabels, err := d.CustomLabels.SelectLabelsByLabelId(label.LabelId, before, limit)
	if err != nil {
		return nil, err
	}

	return customLabels, nil
}

func (d *DBx) InitActorInfo(actorrow *ActorRow, postlabels []*PostLabelRow) error {
	res, err := d.Actors.NamedExec("UPDATE actors SET blocked = :blocked, created_at=:created_at, last_post=:last_post, posts=:posts WHERE actor_id=:actor_id", actorrow)
	if err != nil {
		return err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("could not find actor with did %s", actorrow.Did)
	}

	d.Actors.cache.Add(actorrow.Did, actorrow)

	for _, postlabel := range postlabels {
		err = d.PostLabels.InsertPostLabel(postlabel.PostId, []int64{postlabel.LabelId})
		if err != nil {
			fmt.Printf("error inserting post label %d %d: %+v\n", postlabel.PostId, postlabel.LabelId, err)
		}
	}

	return nil
}

func (d *DBx) LabelPost(postid int64, labels []string) error {
	labelids := make([]int64, 0, len(labels))
	for _, name := range labels {
		label, err := d.Labels.FindOrCreateLabel(name)
		if err != nil {
			return err
		}

		labelids = append(labelids, label.LabelId)
	}

	err := d.PostLabels.InsertPostLabel(postid, labelids)
	if err != nil {
		return err
	}

	err = d.Posts.MarkLabeled(postid)
	if err != nil {
		return err
	}

	return nil
}

func (d *DBx) Close() error {
	errs := ParallelizeFuncsWithRetries(
		func() error { return d.Actors.Close() },
		func() error { return d.Dms.Close() },
		func() error { return d.Follows.Close() },
		func() error { return d.FollowsIndexed.Close() },
		func() error { return d.Labels.Close() },
		func() error { return d.Mentions.Close() },
		func() error { return d.PostLabels.Close() },
		func() error { return d.Posts.Close() },
		func() error { return d.Quotes.Close() },
		func() error { return d.Replies.Close() },
		func() error { return d.ThreadMentions.Close() },
	)
	if len(errs) > 0 {
		msg := fmt.Sprintf("Error closing databases:\n")
		for _, e := range errs {
			msg = fmt.Sprintf("%s\n%s", msg, e)
		}
		log.Print(msg)
		return errors.New(msg)
	}

	return nil
}

func NewDBx(ctx context.Context) *DBx {
	clk, ok := ctx.Value("clock").(clock.Clock)
	if !ok {
		clk = clock.NewClock()
	}

	dir, ok := ctx.Value("db-dir").(string)
	if !ok {
		dir = DefaultDbDir
	}

	err := os.MkdirAll(dir, 0750)
	if err != nil {
		panic(err)
	}

	actorCacheSize, ok := ctx.Value("db-actor-cache-size").(int)
	if !ok {
		actorCacheSize = DefaultDbActorCacheSize
	}

	followCacheSize, ok := ctx.Value("db-follow-cache-size").(int)
	if !ok {
		followCacheSize = DefaultDbFollowCacheSize
	}

	labelCacheSize, ok := ctx.Value("db-label-cache-size").(int)
	if !ok {
		labelCacheSize = DefaultDbLabelCacheSize
	}

	extendedIndexing, _ := ctx.Value("extended-indexing").(bool)

	mmapSize, ok := ctx.Value("db-mmap-size").(int)
	if ok {
		SQLiteMMapSize = mmapSize
	}

	synchronous, ok := ctx.Value("db-synchronous-mode").(string)
	if ok {
		SQLiteSynchronous = synchronous
	}

	autocheckpoint, ok := ctx.Value("db-wal-autocheckpoint").(int)
	if ok {
		SQLiteWalAutocheckpoint = autocheckpoint
	}

	threshold, ok := ctx.Value("slow-query-threshold-ms").(int64)
	if ok {
		SlowQueryThresholdMs = threshold
	}

	signingKeyHex := []byte(ctx.Value("signing-key").(string))

	signingKeyBytes := make([]byte, hex.DecodedLen(len(signingKeyHex)))
	_, err = hex.Decode(signingKeyBytes, signingKeyHex)
	if err != nil {
		panic(err)
	}

	signingKey, err := crypto.ParsePrivateBytesK256(signingKeyBytes)
	if err != nil {
		panic(err)
	}

	d := &DBx{
		Actors:           NewActorTable(dir, actorCacheSize),
		CustomLabels:     NewCustomLabelTable(dir),
		Dms:              NewDmTable(dir),
		Follows:          NewFollowsTable(dir, followCacheSize),
		FollowsIndexed:   NewFollowsIndexedTable(dir),
		Labels:           NewLabelTable(dir, labelCacheSize),
		Likes:            NewLikesTable(dir),
		Mentions:         NewMentionTable(dir),
		Posts:            NewPostTable(dir),
		PostLabels:       NewPostLabelTable(dir),
		Quotes:           NewQuoteTable(dir),
		Replies:          NewReplyTable(dir),
		Reposts:          NewRepostsTable(dir),
		ThreadMentions:   NewThreadMentionTable(dir),
		clock:            clk,
		debug:            cmd.DebuggingEnabled(ctx, "db"),
		extendedIndexing: extendedIndexing,
		SigningKey:       signingKey,
	}

	if PinnedFollowPost == nil {
		PinnedFollowPost, _ = d.Posts.FindByUri(PinnedFollowPostUrl)
	}

	return d
}

func init() {
	sql.Register("sqlite3_default",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				return conn.SetFileControlInt("", sqlite3.SQLITE_FCNTL_MMAP_SIZE, int(SQLiteMMapSize))
			},
		},
	)
}
