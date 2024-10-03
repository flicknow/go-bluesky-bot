package indexer

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/flicknow/go-bluesky-bot/pkg/client"
	"github.com/flicknow/go-bluesky-bot/pkg/clock"
	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/dbx"
	"github.com/flicknow/go-bluesky-bot/pkg/firehose"
	"github.com/flicknow/go-bluesky-bot/pkg/ticker"
	"github.com/flicknow/go-bluesky-bot/pkg/utils"
)

var G1Bot = "did:plc:kzkl2onyewbs7pehh2ellzcb"
var NewsFeedBot = "did:plc:4hm6gb7dzobynqrpypif3dck"
var TickBot = "did:plc:kwmcvt4maab47n7dgvepg4tr"
var REM = "did:plc:3nodfbwjlsd77ckgrodawvpv"

type Indexer struct {
	Client client.Client
	Db     *dbx.DBx

	clock                    clock.Clock
	debug                    bool
	extendedIndexing         bool
	keepSeconds              int64
	pruneChunk               int
	customLabelerTickMinutes int64
	labelTickMinutes         int64
	prunerTickMinutes        int64
	customLabelerTicker      *ticker.Ticker
	labelTicker              *ticker.Ticker
	prunerTicker             *ticker.Ticker
	wg                       *sync.WaitGroup
}

func NewIndexer(ctx context.Context, client client.Client) (*Indexer, error) {
	extendedIndexing, _ := ctx.Value("extended-indexing").(bool)

	clk, ok := ctx.Value("clock").(clock.Clock)
	if !ok {
		clk = clock.NewClock()
	}

	keepDays, ok := ctx.Value("keep-days").(int64)
	if !ok {
		keepDays = 60
	}

	pruneChunk, ok := ctx.Value("prune-chunk").(int)
	if !ok {
		pruneChunk = 30
	}

	customLabelerTickMinutes, ok := ctx.Value("custom-labeler-tick-minutes").(int64)
	if !ok {
		customLabelerTickMinutes = 1
	}

	labelTickMinutes, ok := ctx.Value("label-tick-minutes").(int64)
	if !ok {
		labelTickMinutes = 1
	}

	prunerTickMinutes, ok := ctx.Value("pruner-tick-minutes").(int64)
	if !ok {
		prunerTickMinutes = 1
	}

	indexer := &Indexer{
		Client:                   client,
		clock:                    clk,
		debug:                    cmd.DebuggingEnabled(ctx, "indexer"),
		keepSeconds:              keepDays * 24 * 60 * 60,
		pruneChunk:               pruneChunk,
		customLabelerTickMinutes: customLabelerTickMinutes,
		labelTickMinutes:         labelTickMinutes,
		prunerTickMinutes:        prunerTickMinutes,
		extendedIndexing:         extendedIndexing,
		wg:                       &sync.WaitGroup{},
	}

	indexer.Db = dbx.NewDBx(ctx)

	return indexer, nil
}

func (i *Indexer) Start() {
	if (i.customLabelerTicker == nil) && (i.customLabelerTickMinutes != 0) {
		i.customLabelerTicker = ticker.NewTicker(time.Duration(i.customLabelerTickMinutes) * time.Minute)
	}
	go i.runCustomLabeler()

	if (i.labelTicker == nil) && (i.labelTickMinutes != 0) {
		i.labelTicker = ticker.NewTicker(time.Duration(i.labelTickMinutes) * time.Minute)
	}
	go i.runLabeler()

	if (i.prunerTicker == nil) && (i.prunerTickMinutes != 0) {
		time.Minute.Minutes()
		i.prunerTicker = ticker.NewTicker(time.Duration(i.prunerTickMinutes) * time.Minute)
	}
	go i.runPruner()
}

func (i *Indexer) Stop() {
	if i.customLabelerTicker != nil {
		customLabelerTicker := i.customLabelerTicker
		i.customLabelerTicker = nil
		customLabelerTicker.Stop()
	}
	if i.labelTicker != nil {
		labelTicker := i.labelTicker
		i.labelTicker = nil
		labelTicker.Stop()
	}
	if i.prunerTicker != nil {
		prunerTicker := i.prunerTicker
		i.prunerTicker = nil
		prunerTicker.Stop()
	}

	i.wg.Wait()

	if i.Db != nil {
		i.Db.Close()
	}
}

func (i *Indexer) IndexFollows(rateLimit int64, actor *dbx.ActorRow) (int64, error) {
	actorid := actor.ActorId
	cli := i.Client
	d := i.Db

	indexed, err := d.FollowsIndexed.FindOrCreateByActorId(actorid)
	if err != nil {
		return 0, err
	}
	if (indexed != nil) && (indexed.LastFollow >= 0) {
		return 0, nil
	}

	var hits int64 = 0
	var lastid int64 = 0
	cursor := indexed.Cursor
	done := false
	now := i.clock.NowUnix()
	for !done {
		records, c, err := cli.GetFollows(actor.Did, 100, cursor)
		if err != nil {
			if strings.Contains(err.Error(), "InvalidRequest: Could not find repo") {
				err = d.FollowsIndexed.SetLastFollow(actorid, 0)
			}
			return hits, err
		}

		hits++
		if len(records) == 0 {
			done = true
			break
		}

		follows := make([]*dbx.FollowRow, len(records))
		for n, record := range records {
			follow := &bsky.GraphFollow{}
			r, ok := record.Value.Val.(*bsky.GraphFollow)
			if ok {
				follow = r
			} else {
				err := utils.DecodeCBOR(record.Value.Val, &follow)
				if err != nil {
					log.Printf("error decoding %s: %+v", record.Uri, err)
					return 0, nil
				}
			}

			subject, err := d.Actors.FindOrCreateActor(follow.Subject)
			if err != nil {
				return 0, err
			}

			follows[n] = &dbx.FollowRow{
				ActorId:   actorid,
				Rkey:      utils.ParseRkey(record.Uri),
				SubjectId: subject.ActorId,
				CreatedAt: now,
			}
		}

		last, err := d.Follows.InsertFollow(follows...)
		if err != nil {
			return 0, err
		}

		lastid = last.FollowId

		if c == "" {
			done = true
			break
		} else {
			cursor = c
		}

		if (hits >= rateLimit) || (i.labelTicker == nil) {
			return hits, d.FollowsIndexed.SetCursor(actor.ActorId, cursor)
		}
	}

	if lastid == 0 {
		last, err := d.Follows.FindLastFollow(actorid)
		if err != nil {
			return hits, err
		}
		if last != nil {
			lastid = last.FollowId
		}
	}

	_, err = d.FollowsIndexed.FindOrCreateByActorId(actorid)
	if err != nil {
		return hits, err
	}

	err = d.FollowsIndexed.SetLastFollow(actorid, lastid)
	if err != nil {
		return hits, err
	}

	return hits, nil
}

func (i *Indexer) runPruner() {
	wg := i.wg
	wg.Add(1)
	defer wg.Done()

	chunk := i.pruneChunk
	ticker := i.prunerTicker
	if ticker == nil {
		return
	}

	for range ticker.C {
		since := i.clock.NowUnix() - (i.keepSeconds)

		totalPruned := 0
	PRUNE:
		for {
			if i.prunerTicker == nil {
				break PRUNE
			}

			pruned, err := i.Db.Prune(since, chunk)
			if err != nil {
				log.Printf("error pruning: %+v", err)
			}
			if i.debug && (totalPruned == 0) && (pruned != 0) {
				fmt.Printf("> starting prune!\n")
			}
			totalPruned += pruned
			if pruned < chunk {
				if i.debug && (totalPruned != 0) {
					fmt.Printf("< pruned %d records\n", totalPruned)
				}
				break PRUNE
			}

		}

	}
}

func (i *Indexer) runLabelerOnceForLabels(rateLimit int64) (int64, error) {
	cutoff := i.clock.NowUnix() - (5 * 60)
	var labelHits int64 = 0

	for {
		if i.labelTicker == nil {
			return labelHits, nil
		}

		posts, err := i.BatchLabel(cutoff, 25)
		if err != nil {
			return labelHits, err
		}
		labelHits++

		if (posts == nil) || (len(posts) < 20) {
			return labelHits, nil
		}

		if labelHits >= rateLimit {
			return labelHits, nil
		}
	}
}

func (i *Indexer) runLabelerOnceForActors(rateLimit int64) (int64, error) {
	var actorHits int64 = 0
	var lastActorId int64 = 0

	for {
		if i.labelTicker == nil {
			return actorHits, nil
		}

		actors, hits, err := i.InitUninitializedActors(rateLimit-actorHits, lastActorId, 25)
		if err != nil {
			return actorHits, err
		}
		actorHits += hits

		if (actors == nil) || (len(actors) == 0) {
			return actorHits, nil
		}

		if actorHits >= rateLimit {
			return actorHits, nil
		}

		lastActorId = actors[len(actors)-1].ActorId
	}
}

func (i *Indexer) runLabelerOnceForFollows(rateLimit int64) (int64, error) {
	var followHits int64 = 0

	for {
		if i.labelTicker == nil {
			return followHits, nil
		}

		rows, err := i.Db.FollowsIndexed.SelectUnindexed(int(rateLimit))
		if err != nil {
			return followHits, err
		}
		if (rows == nil) || (len(rows) == 0) {
			return followHits, nil
		}

		for _, row := range rows {
			actor, err := i.Db.Actors.FindActorById(row.ActorId)
			if err != nil {
				return followHits, err
			}
			if actor.Blocked {
				err := i.Db.FollowsIndexed.SetLastFollow(actor.ActorId, 0)
				if err != nil {
					log.Printf("error indexing follows for blocked actor %d: %+v", actor.ActorId, err)
				}
				continue
			}

			c, err := i.IndexFollows(rateLimit-followHits, actor)
			followHits += c
			if err != nil {
				log.Printf("error indexing follows for actor %d: %+v", actor.ActorId, err)
				continue
			}

			if followHits >= rateLimit {
				return followHits, nil
			}
		}
	}
}

func (i *Indexer) runLabelerOnce(rateLimit int64) {
	remaining := rateLimit

	actorHits, err := i.runLabelerOnceForActors(remaining / 2)
	if err != nil {
		fmt.Printf("error indexing actors: %+v\n", err)
	}
	if i.debug {
		fmt.Printf("> indexed %d/%d actors\n", actorHits, remaining/2)
	}
	remaining -= actorHits

	followHits, err := i.runLabelerOnceForFollows(remaining)
	if err != nil {
		fmt.Printf("error indexing follows: %+v\n", err)
	}
	if i.debug {
		fmt.Printf("> indexed %d/%d follows\n", followHits, remaining)
	}
}

func (i *Indexer) runCustomLabeler() {
	wg := i.wg
	wg.Add(1)
	defer wg.Done()

	ticker := i.customLabelerTicker
	if ticker == nil {
		return
	}

	for range ticker.C {
		i.runCustomLabelerOnce()
	}
}

func (i *Indexer) InitializeActorBirthdays(limit int64) error {
	cutoff := int64(0)
	chunk := 100

	count := int64(0)
	for {
		if i.customLabelerTicker == nil {
			return nil
		}

		actors, err := i.InitializeActorBirthdaysOnce(cutoff, chunk)
		if err != nil {
			return err
		}
		if len(actors) < chunk {
			return nil
		}

		if limit != 0 {
			count += int64(len(actors))
			if count >= limit {
				return nil
			}
		}
	}
}

func (i *Indexer) InitializeActorBirthdaysOnce(cutoff int64, chunk int) ([]*dbx.ActorRow, error) {
	actors, err := i.Db.Actors.SelectActorsWithoutBirthdays(cutoff, chunk)
	if err != nil {
		return nil, err
	}
	if len(actors) == 0 {
		return []*dbx.ActorRow{}, nil
	}
	for _, actor := range actors {
		did := actor.Did
		createdAt, err := LookupDidPlcCreatedAt(did)
		if err != nil {
			return nil, err
		}

		if createdAt == 0 {
			actor.Blocked = true
			err = i.Db.InitActorInfo(actor, []*dbx.PostLabelRow{})
			if err != nil {
				return nil, err
			}
			continue
		}

		_, err = i.Db.Actors.InitializeBirthday(did, createdAt)
		if err != nil {
			return nil, err
		}
	}

	return actors, nil
}

func (i *Indexer) runCustomLabelerOnce() {
	errs := utils.ParallelizeFuncs(
		func() error {
			err := i.InitializeActorBirthdays(0)
			if err != nil {
				return err
			}

			return nil
		},
		func() error {
			clock := i.clock
			err := i.Db.RecordBirthdayLabels(clock)
			if err != nil {
				return err
			}

			err = i.Db.RecordUnbirthdayLabels(clock)
			if err != nil {
				return err
			}

			err = i.Db.PruneCustomLabels(clock)

			return nil
		},
	)
	if len(errs) > 0 {
		msg := "Error running custom labeler:"
		for _, e := range errs {
			msg = fmt.Sprintf("%s\n%s", msg, e)
		}
		log.Print(msg)
	}
}

func (i *Indexer) runLabeler() {
	wg := i.wg
	wg.Add(1)
	defer wg.Done()

	rateLimit := float64(i.labelTickMinutes) * 2750.0 / 5.0
	ticker := i.labelTicker
	if ticker == nil {
		return
	}

	for range ticker.C {
		i.runLabelerOnce(int64(rateLimit))
	}
}

func (i *Indexer) BatchLabel(cutoff int64, limit int) ([]bsky.FeedDefs_PostView, error) {
	db := i.Db
	rows, err := db.Posts.SelectUnlabeled(cutoff, limit)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}

	var uris []string
	for _, row := range rows {
		uris = append(uris, row.Uri)
	}

	posts, err := i.Client.GetPosts(uris)
	if err != nil {
		fmt.Println(utils.Dump(uris))
		return nil, err
	}

	postsByUri := make(map[string]bsky.FeedDefs_PostView)
	for _, post := range posts {
		postsByUri[post.Uri] = post
	}

	for _, row := range rows {
		post, ok := postsByUri[row.Uri]
		if !ok {
			err := db.LabelPost(row.PostId, []string{})
			if err != nil {
				return nil, err
			}
			continue
		}

		found := make(map[string]bool)
		for _, label := range post.Labels {
			found[label.Val] = true
		}

		labels := make([]string, 0, len(found))
		for label := range found {
			labels = append(labels, label)
		}

		if (post.Author != nil) && (post.Author.Did == REM) {
			if len(labels) > 0 {
				labels = append(labels, "rembangs")
			}
		}

		err := db.LabelPost(row.PostId, labels)
		if err != nil {
			return nil, err
		}
	}

	return posts, nil
}

func (i *Indexer) Label(labels []*atproto.LabelDefs_Label) error {
	uriToPostId := make(map[string]int64)
	postIdToLabels := make(map[int64][]string)

	for _, label := range labels {
		uri := label.Uri

		postid := uriToPostId[uri]
		if postid == 0 {
			var err error
			postid, err = i.Db.Posts.FindPostIdByUri(uri)
			if err != nil {
				return err
			}
			if postid == 0 {
				continue
			}

			uriToPostId[uri] = postid
		}

		labels := postIdToLabels[postid]
		if labels == nil {
			labels = make([]string, 0, 1)
		}

		postIdToLabels[postid] = append(labels, label.Val)
	}

	for postid, labels := range postIdToLabels {
		err := i.Db.LabelPost(postid, labels)
		if err != nil {
			return err
		}
	}

	return nil
}

func (i *Indexer) InitUninitializedActors(rateLimit int64, cutoff int64, limit int) ([]*dbx.ActorRow, int64, error) {
	if rateLimit <= 0 {
		return []*dbx.ActorRow{}, 0, nil
	}

	d := i.Db
	actors, err := i.Db.Actors.SelectUninitializedActors(cutoff, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("error selecting uninitialized actors: %w", err)
	}
	if (actors == nil) || (len(actors) == 0) {
		return nil, 0, nil
	}

	dids := make([]string, 0, len(actors))
	rowsByDid := make(map[string]*dbx.ActorRow)
	for _, actor := range actors {
		if !((len(actor.Did) > 8) && (actor.Did[:8] == "did:plc:")) {
			fmt.Printf("> blocking non plc did: %s\n", actor.Did)

			actor.Blocked = true
			err := d.InitActorInfo(actor, []*dbx.PostLabelRow{})
			if err != nil {
				return nil, 0, err
			}

			continue
		}

		dids = append(dids, actor.Did)
		rowsByDid[actor.Did] = actor
	}

	var count int64 = 0
	profiles, err := i.Client.GetActors(dids)
	count++

	if count >= rateLimit {
		return []*dbx.ActorRow{}, count, nil
	}

	if err != nil {
		profiles = []*bsky.ActorDefs_ProfileViewDetailed{}
		for _, did := range dids {
			profile, err := i.Client.GetActor(did)
			count++

			if (count >= rateLimit) || (i.labelTicker == nil) {
				return []*dbx.ActorRow{}, count, nil
			}

			actorrow := rowsByDid[did]
			if (err != nil) && (strings.Contains(err.Error(), "AccountTakedown") || strings.Contains(err.Error(), "BlockedActor") || strings.Contains(err.Error(), "BlockedByActor") || strings.Contains(err.Error(), "Profile not found")) {
				actorrow.Blocked = true
				actorrow.CreatedAt = -1
				err := d.InitActorInfo(actorrow, []*dbx.PostLabelRow{})
				if err != nil {
					log.Printf("error initializing actor %+v: %+v", actorrow, err)
				}
				continue
			} else if (profile != nil) && (profile.Handle == "handle.invalid") {
				actorrow.CreatedAt = -1
				err := d.InitActorInfo(actorrow, []*dbx.PostLabelRow{})
				if err != nil {
					log.Printf("error initializing actor %+v: %+v", actorrow, err)
				}
				continue
			} else if err != nil {
				fmt.Printf("error getting actor %s: %+v\n", actorrow.Did, err)
				continue
			}

			if profile != nil {
				profiles = append(profiles, profile)
			}
		}
	}

	indexed := make(map[string]bool)
	for _, profile := range profiles {
		actor := rowsByDid[profile.Did]
		indexed[profile.Did] = true

		if (profile != nil) && (profile.Handle == "handle.invalid") {
			actor.CreatedAt = -1
			err := d.InitActorInfo(actor, []*dbx.PostLabelRow{})
			if err != nil {
				log.Printf("error initializing actor %+v: %+v", actor, err)
			}
			continue
		}

		hits, err := i.InitActorInfo(actor, profile)
		if err != nil {
			log.Printf("error initializing actor %+v: %+v", actor, err)
		}
		count += hits

		if (count >= rateLimit) || (i.labelTicker == nil) {
			return []*dbx.ActorRow{}, count, nil
		}
	}

	for _, actor := range actors {
		if !indexed[actor.Did] {
			actor.CreatedAt = -1
			err := d.InitActorInfo(actor, []*dbx.PostLabelRow{})
			if err != nil {
				log.Printf("error initializing actor %+v: %+v", actor, err)
			}
		}
	}

	return actors, count, nil
}

func (i *Indexer) Delete(uri string) error {
	db := i.Db

	var err error
	if strings.Contains(uri, "app.bsky.feed.like") {
		err = db.DeleteLike(uri)
	} else if strings.Contains(uri, "app.bsky.feed.post") {
		err = db.DeletePost(uri)
	} else if strings.Contains(uri, "app.bsky.feed.repost") {
		err = db.DeleteRepost(uri)
	} else if strings.Contains(uri, "app.bsky.graph.follow") {
		//err = db.DeleteFollow(uri)
	}
	if err != nil {
		log.Printf("error deleting %s: %+v\n", uri, err)
		return err
	}

	return nil
}

func (i *Indexer) Like(likeRef *firehose.LikeRef) error {
	uri := likeRef.Ref.Uri
	did := utils.ParseDid(uri)
	isMark := did == dbx.MARK

	if !(i.extendedIndexing || isMark) {
		return nil
	}

	return i.Db.InsertLike(likeRef)
}

func (i *Indexer) InitActorInfo(actorrow *dbx.ActorRow, profile *bsky.ActorDefs_ProfileViewDetailed) (int64, error) {
	db := i.Db
	var hits int64 = 0

	if actorrow.CreatedAt != 0 {
		return 0, db.InitActorInfo(actorrow, []*dbx.PostLabelRow{})
	}
	if actorrow.Blocked {
		actorrow.Blocked = true
		return 0, db.InitActorInfo(actorrow, []*dbx.PostLabelRow{})
	}

	// only lookup plc dids
	if !((len(actorrow.Did) > 8) && (actorrow.Did[:8] == "did:plc:")) {
		fmt.Printf("> blocking non plc did: %s\n", actorrow.Did)
		actorrow.Blocked = true
		return 0, db.InitActorInfo(actorrow, []*dbx.PostLabelRow{})
	}

	client := i.Client

	var err error = nil
	if profile == nil {
		actorrow.CreatedAt = -1
		return hits, db.InitActorInfo(actorrow, []*dbx.PostLabelRow{})
	}

	viewer := profile.Viewer
	if viewer != nil && (viewer.BlockedBy != nil) && *viewer.BlockedBy {
		actorrow.Blocked = true
		actorrow.CreatedAt = -1
		return hits, db.InitActorInfo(actorrow, []*dbx.PostLabelRow{})
	}

	if profile.IndexedAt != nil {
		actorrow.CreatedAt, err = toUnixTime(*profile.IndexedAt)
		if err != nil {
			actorrow.CreatedAt = i.clock.NowUnix()
			log.Printf("error parsing profile indexedAt for actor %s: %+v\n", actorrow.Did, err)
		}
	} else {
		actorrow.CreatedAt = i.clock.NowUnix()
	}

	var totalPosts int64 = 0
	if profile.PostsCount != nil {
		totalPosts = *profile.PostsCount
		actorrow.Posts = totalPosts
	}

	var postLimit int64 = 50
	var firstPost *bsky.FeedDefs_FeedViewPost = nil
	if totalPosts != 0 {
		posts, _, err := client.GetAuthorFeed(profile.Handle, "posts_no_replies", postLimit, "")
		hits++

		if (err != nil) && (strings.Contains(err.Error(), "BlockedActor") || strings.Contains(err.Error(), "BlockedByActor")) {
			actorrow.Blocked = true
			return hits, db.InitActorInfo(actorrow, []*dbx.PostLabelRow{})
		} else if err != nil {
			return hits, fmt.Errorf("error getting author feed for %s: %w", actorrow.Did, err)
		}
		if (posts == nil) || (len(posts) == 0) {
			actorrow.Posts = 0
			return hits, db.InitActorInfo(actorrow, []*dbx.PostLabelRow{})
		}

		var originalPosts int64 = 0
		var lastPost *bsky.FeedDefs_FeedViewPost = nil
		for _, post := range posts {
			if post.Post.Author.Did != actorrow.Did {
				continue
			}
			if (post.Reply == nil) || (post.Reply.Parent == nil) {
				// it's reverse chronological order, so first is seen last, last is first
				originalPosts = originalPosts + 1
				firstPost = post
				if lastPost == nil {
					lastPost = post
				}
			}
		}
		// if we haven't seen a recent post, and didn't already record one
		// just use their earliest post in the window we have
		if (actorrow.LastPost == 0) && (lastPost == nil) && (totalPosts > postLimit) {
			lastPost = posts[len(posts)-1]
		}
		if lastPost != nil {
			actorrow.LastPost, err = toUnixTime(lastPost.Post.IndexedAt)
			if err != nil {
				return hits, fmt.Errorf("error parsing last post indexAt for actor %s post %s: %w", actorrow.Did, lastPost.Post.Uri, err)
			}
		}
		// if we haven't seen all their posts, just use total posts
		if totalPosts <= postLimit {
			actorrow.Posts = originalPosts
		} else {
			// we probably didn't see the actual first post if there were multiple pages of results
			firstPost = nil
			actorrow.Posts = totalPosts
		}
	}

	postlabels := make([]*dbx.PostLabelRow, 0, 1)
	if (totalPosts <= postLimit) && (firstPost != nil) {
		firstPostId, err := db.Posts.FindPostIdByUri(firstPost.Post.Uri)
		if err != nil {
			return hits, err
		}

		if firstPostId != 0 {
			newskieRow, err := db.Labels.FindOrCreateLabel("newskie")
			if err != nil {
				return hits, err
			}

			if (newskieRow != nil) && (newskieRow.LabelId != 0) {
				postlabel := &dbx.PostLabelRow{PostId: firstPostId, LabelId: newskieRow.LabelId}
				postlabels = append(postlabels, postlabel)
			}
		}
	}

	err = db.InitActorInfo(actorrow, postlabels)
	if err != nil {
		return hits, err
	}

	return hits, nil
}

func (i *Indexer) Newskie(newskie string) error {
	db := i.Db
	actor, err := db.Actors.FindOrCreateActor(newskie)
	if err != nil {
		log.Printf("error inserting newskie %s: %+v\n", newskie, err)
		return err
	}

	if actor.Created {
		actor.CreatedAt = i.clock.NowUnix()
		err := db.InitActorInfo(actor, []*dbx.PostLabelRow{})
		if err != nil {
			log.Printf("error initializing newskie actor info %s: %+v\n", newskie, err)
			return nil
		}

		_, err = db.FollowsIndexed.FindOrCreateByActorId(actor.ActorId)
		if err != nil {
			log.Printf("error creating newskie follow index %s: %+v\n", newskie, err)
			return nil
		}

		err = db.FollowsIndexed.SetLastFollow(actor.ActorId, 0)
		if err != nil {
			log.Printf("error indexing newskie follow %s: %+v\n", newskie, err)
			return nil
		}
	}

	return nil
}

func (i *Indexer) Block(blockRef *firehose.BlockRef) error {
	did := utils.ParseDid(blockRef.Ref.Uri)
	if did != "" {
		return nil
	}

	return i.Db.Block(did)
}

func (i *Indexer) Follow(follow *firehose.FollowRef) error {
	return i.Db.InsertFollow(follow)
}

func (i *Indexer) Post(postRef *firehose.PostRef) (*dbx.PostRow, error) {
	did := utils.ParseDid(postRef.Ref.Uri)
	if (did == G1Bot) || (did == NewsFeedBot) || (did == TickBot) {
		return nil, nil
	}

	labels := make([]string, 0, 2)
	if IsCeuSemLimites(postRef.Post.Text) {
		labels = append(labels, "ceusemlimites")
	}

	actor, err := i.Db.Actors.FindOrCreateActor(did)
	if err != nil {
		log.Printf("error finding or creating actor for post %s: %+v\n", postRef.Ref.Uri, err)
		return nil, err
	}

	if !actor.Blocked && (actor.CreatedAt != 0) && ((postRef.Post.Reply == nil) || (postRef.Post.Reply.Parent == nil)) {
		if actor.Posts == 0 {
			labels = append(labels, "newskie")
		}
		if (actor.LastPost > 0) && (actor.LastPost <= (i.clock.NowUnix() - (14 * 24 * 60 * 60))) {
			labels = append(labels, "renewskie")
		}
	}

	if (postRef.Post.Reply == nil) && isGm(postRef.Post) {
		labels = append(labels, "gmgn")
	}

	if strings.Contains(postRef.Post.Text, "‼") && (actor.Did == REM) {
		labels = append(labels, "rembangs")
	}

	selfLabels := postRef.Post.Labels
	if (selfLabels != nil) && (selfLabels.LabelDefs_SelfLabels != nil) && (selfLabels.LabelDefs_SelfLabels.Values != nil) {
		for _, value := range selfLabels.LabelDefs_SelfLabels.Values {
			if (value != nil) && (value.Val != "") {
				labels = append(labels, value.Val)
			}
		}
	}

	post, err := i.Db.InsertPost(postRef, actor, labels...)
	if err != nil {
		log.Printf("error inserting post %s: %+v\n", postRef.Ref.Uri, err)
		return nil, err
	}

	return post, nil
}

func (i *Indexer) Repost(repostRef *firehose.RepostRef) error {
	if !i.extendedIndexing {
		return nil
	}

	return i.Db.InsertRepost(repostRef)
}

func toUnixTime(timestamp string) (int64, error) {
	t, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		return fixTimestamp(timestamp)
	}

	return t.UTC().Unix(), nil
}

func fixTimestamp(timestamp string) (int64, error) {
	if len(timestamp) < 20 {
		fmt.Printf("got short timestamp: %s\n", timestamp)
		return clock.NewClock().NowUnix(), nil
	}

	t, err := time.Parse(time.RFC3339, timestamp[:19]+"Z")
	if err != nil {
		return 0, err
	}

	now := time.Now().UTC()
	t = t.UTC()

	for now.Before(t) {
		t = t.Add(-1 * time.Hour)
	}

	return t.Unix(), nil
}
