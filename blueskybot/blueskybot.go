package blueskybot

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/flicknow/go-bluesky-bot/pkg/client"
	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/dbx"
	"github.com/flicknow/go-bluesky-bot/pkg/firehose"
	"github.com/flicknow/go-bluesky-bot/pkg/indexer"
	"github.com/flicknow/go-bluesky-bot/pkg/ticker"
	"github.com/flicknow/go-bluesky-bot/pkg/utils"
	cli "github.com/urfave/cli/v2"
)

func chanEmpty(chs ...any) bool {
	for _, ch := range chs {
		rv := reflect.ValueOf(ch)
		if rv.Kind() != reflect.Chan {
			continue
		}

		if rv.Len() != 0 {
			return false
		}
	}

	return true
}

var BlueskyBot = &cli.Command{
	Name: "run",
	Flags: cmd.CombineFlags(
		cmd.WithClient,
		cmd.WithIndexer,
		cmd.WithServer,
	),
	Action: func(cctx *cli.Context) error {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
		defer stop()

		go func() {
			log.Println(http.ListenAndServe(":6060", nil))
		}()

		client, err := client.NewClient(cctx)
		if err != nil {
			return err
		}

		indexer, err := indexer.NewIndexer(cmd.ToContext(cctx), client)
		if err != nil {
			return err
		}

		indexer.Start()
		defer indexer.Stop()

		server := NewServer(cmd.ToContext(cctx), indexer)
		go server.Serve()
		defer server.Shutdown(ctx)

		hose := firehose.NewFirehose(cmd.ToContext(cctx))
		fCh, err := hose.Start(ctx)
		if err != nil {
			return err
		}

		labeler := firehose.NewLabelerFirehose(cmd.ToContext(cctx))
		lCh, err := labeler.Start(ctx)
		if err != nil {
			return err
		}
		defer func() {
			hose.Stop()
			labeler.Stop()
		}()

		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("caught exception: %#v\n", r)
				panic(r)
			}
		}()

		var lastFirehosePing int64 = 0
		var lastLabelerPing int64 = 0
		var lastPostId int64 = 0
		var lastFirehoseSeen int64 = 0
		var lastLabelerSeen int64 = 0
		shutdown := false

		pinger := ticker.NewTicker(1 * time.Minute)
		go func() {
			for range pinger.C {
				log.Printf("> last seen post=%d, seq=%d (%d)\n", lastPostId, lastFirehoseSeen, lastFirehoseSeen-lastFirehosePing)
				if lastFirehosePing == lastFirehoseSeen {
					panic("restarting firehose!")
				}
				if lastLabelerPing == lastLabelerSeen {
					fmt.Println("> restarting labeler!")
					var err error
					lCh, err = labeler.Restart(ctx)
					if err != nil {
						panic(err)
					}
				}
				lastFirehosePing = lastFirehoseSeen
				lastLabelerPing = lastLabelerSeen
			}
		}()
		defer func() { pinger.Stop() }()

		var fEvt *firehose.FirehoseEvent
		var lEvt *firehose.LabelerEvent
		var post *dbx.PostRow
		var seq int64
	LOOP:
		for !shutdown {
			select {
			case <-ctx.Done():
				fmt.Println("Interrupt!")
				return nil
			case lEvt = <-lCh:
				if lEvt == nil {
					fmt.Println("> END OF LOOP")
					break LOOP
				}

				switch lEvt.Type {
				case firehose.EvtKindError:
					err = lEvt.Error
					if err == nil {
						continue
					}
					if errors.Is(err, context.Canceled) {
						continue
					}

					if errors.Is(err, firehose.ErrFatal) {
						log.Printf("received labeler firehose error: %+v, restarting\n", err)

						var err error
						lCh, err = labeler.Restart(ctx)
						if err != nil {
							return err
						}
					} else {
						log.Printf("received labeler firehose error: %+v\n", err)
					}
				case firehose.EvtKindLabelerInfo:
					if lEvt.Info == nil {
						continue
					}
					fmt.Println(utils.Dump(lEvt.Info))
				case firehose.EvtKindLabel:
					if lEvt.Labels == nil {
						continue
					}

					err = dbx.RetryDbIsLocked(func() error { return indexer.Label(lEvt.Labels.Labels) })()
					if err != nil {
						continue
					}
				}

				seq = lEvt.Seq
				if seq != 0 {
					labeler.Ack(seq)
					lastLabelerSeen = seq
					if lastLabelerPing == 0 {
						lastLabelerPing = seq
					}
				}
			case fEvt = <-fCh:
				if fEvt == nil {
					fmt.Println("> END OF LOOP")
					break LOOP
				}

				switch fEvt.Type {
				case firehose.EvtKindError:
					err = fEvt.Error
					if err == nil {
						continue
					}
					if errors.Is(err, context.Canceled) {
						continue
					}

					if errors.Is(err, firehose.ErrFatal) {
						log.Printf("received firehose error: %+v, restarting\n", err)

						var err error
						fCh, err = hose.Restart(ctx)
						if err != nil {
							return err
						}
					} else {
						log.Printf("received firehose error: %+v\n", err)
					}
				case firehose.EvtKindFirehoseLike:
					if fEvt.Like == nil {
						continue
					}

					err = dbx.RetryDbIsLocked(func() error { return indexer.Like(fEvt.Like) })()
					if err != nil {
						continue
					}
				case firehose.EvtKindFirehosePost:
					if fEvt.Post == nil {
						continue
					}

					err = dbx.RetryDbIsLocked(func() error {
						post, err = indexer.Post(fEvt.Post)
						if err != nil {
							return err
						}

						if (post != nil) && (post.PostId != 0) {
							lastPostId = post.PostId
						}

						return nil
					})()
					if err != nil {
						fmt.Printf("> ERROR INDEXING SEQ %d POST %s: %+v\n", fEvt.Seq, fEvt.Post.Ref.Uri, err)
						continue
					}
				case firehose.EvtKindFirehoseRepost:
					if fEvt.Repost == nil {
						continue
					}

					err = dbx.RetryDbIsLocked(func() error { return indexer.Repost(fEvt.Repost) })()
					if err != nil {
						continue
					}
				case firehose.EvtKindFirehoseProfile:
					if fEvt.Profile == "" {
						continue
					}

					err = dbx.RetryDbIsLocked(func() error { return indexer.Newskie(fEvt.Profile) })()
					if err != nil {
						continue
					}
				case firehose.EvtKindFirehoseBlock:
					if fEvt.Block == nil {
						continue
					}

					err = dbx.RetryDbIsLocked(func() error { return indexer.Block(fEvt.Block) })()
					if err != nil {
						continue
					}
					/*
						case firehose.EvtKindFirehoseFollow:
							followRef := fEvt.Follow
							if followRef == nil {
								continue
							}

								err := dbx.RetryDbIsLocked(func() error { return indexer.Follow(followRef) })()
								if err != nil {
									continue
								}
					*/
				case firehose.EvtKindFirehoseDelete:
					if fEvt.Delete == "" {
						continue
					}

					err = dbx.RetryDbIsLocked(func() error { return indexer.Delete(fEvt.Delete) })()
					if err != nil {
						continue
					}
				case firehose.EvtKindFirehoseTombstone:
					if fEvt.Tombstone == "" {
						continue
					}

					blockRef := &firehose.BlockRef{
						Subject: client.Did(),
						Ref: &atproto.RepoStrongRef{
							Uri: fmt.Sprintf("at://%s/tombstone/tombstone", fEvt.Tombstone),
						},
					}

					err = dbx.RetryDbIsLocked(func() error { return indexer.Block(blockRef) })()
					if err != nil {
						continue
					}
				}

				seq = fEvt.Seq
				if seq != 0 {
					hose.Ack(seq)
					lastFirehoseSeen = seq
					if lastFirehosePing == 0 {
						lastFirehosePing = seq
					}
				}

			}
		}

		return nil
	},
}
