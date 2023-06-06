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

	"github.com/flicknow/go-bluesky-bot/pkg/client"
	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/dbx"
	"github.com/flicknow/go-bluesky-bot/pkg/firehose"
	"github.com/flicknow/go-bluesky-bot/pkg/indexer"
	"github.com/flicknow/go-bluesky-bot/pkg/ticker"
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

		firehose := firehose.NewFirehose(cmd.ToContext(cctx), client)
		errCh := firehose.Errors()
		blockCh := firehose.Blocks()
		deleteCh := firehose.Deletes()
		followCh := firehose.Follows()
		likeCh := firehose.Likes()
		newskiesCh := firehose.Newskies()
		postCh := firehose.Posts()
		repostCh := firehose.Reposts()

		server := NewServer(cctx.String("listen"), indexer, cctx.Int64("max-web-connections"), cctx.String("pinned-post"))
		go server.Serve()
		defer server.Shutdown(context.Background())

		err = firehose.Start(ctx)
		if err != nil {
			return err
		}
		log.Printf("STARTING cursor = %d\n", firehose.Cursor)
		defer func() {
			firehose.Stop()
			log.Printf("STOPPED cursor = %d\n", firehose.Cursor)
		}()

		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("caught exception: %#v\n", r)
				panic(r)
			}
		}()

		// run until we're shutting down and all the channels are empty
		var lastPing int64 = firehose.Cursor
		var lastPostId int64 = 0
		var lastSeen int64 = 0
		pinger := ticker.NewTicker(1 * time.Minute)
		shutdown := false
		for !(shutdown && chanEmpty(errCh, blockCh, deleteCh, followCh, likeCh, newskiesCh, postCh, repostCh)) {
			select {
			case err := <-errCh:
				if (err != nil) && (!errors.Is(err, context.Canceled)) {
					log.Printf("received firehose error: %+v, restarting\n", err)
					if shutdown {
						firehose.CloseChannels()
					} else {
						err := firehose.Restart(ctx)
						if err != nil {
							return err
						}
					}
				}
			case blockRef := <-blockCh:
				if blockRef != nil {
					lastSeen = blockRef.Seq
					err := dbx.RetryDbIsLocked(func() error { return indexer.Block(blockRef) })
					if err == nil {
						firehose.Ack(blockRef.Seq)
					}
				}
			case deleted := <-deleteCh:
				if deleted != nil {
					lastSeen = deleted.Seq
					err := dbx.RetryDbIsLocked(func() error { return indexer.Delete(deleted.Uri) })
					if err == nil {
						firehose.Ack(deleted.Seq)
					}
				}
			case follow := <-followCh:
				if follow != nil {
					lastSeen = follow.Seq
					err := dbx.RetryDbIsLocked(func() error { return indexer.Follow(follow) })
					if err == nil {
						firehose.Ack(follow.Seq)
					}

				}
			case likeRef := <-likeCh:
				if likeRef != nil {
					lastSeen = likeRef.Seq
					err := dbx.RetryDbIsLocked(func() error { return indexer.Like(likeRef) })
					if err == nil {
						firehose.Ack(likeRef.Seq)
					}
				}
			case newskie := <-newskiesCh:
				if newskie != nil {
					lastSeen = newskie.Seq
					err := dbx.RetryDbIsLocked(func() error { return indexer.Newskie(newskie.Uri) })
					if err == nil {
						firehose.Ack(newskie.Seq)
					}
				}
			case postRef := <-postCh:
				if postRef != nil {
					lastSeen = postRef.Seq
					err = dbx.RetryDbIsLocked(func() error {
						post, err := indexer.Post(postRef)
						if err != nil {
							return err
						}
						if post != nil {
							lastPostId = post.PostId
						}
						return nil
					})
					if err == nil {
						firehose.Ack(postRef.Seq)
					}
				}
			case repostRef := <-repostCh:
				if repostRef != nil {
					lastSeen = repostRef.Seq
					err := dbx.RetryDbIsLocked(func() error { return indexer.Repost(repostRef) })
					if err == nil {
						firehose.Ack(repostRef.Seq)
					}
				}
			case <-pinger.C:
				log.Printf("> last seen post=%d, seq=%d (%d)\n", lastPostId, lastSeen, lastSeen-lastPing)
				if lastPing == lastSeen {
					fmt.Println("> restarting!")
					err := firehose.Restart(ctx)
					if err != nil {
						return err
					}
				}
				lastPing = lastSeen
				report := dbx.Collector.Report()
				if report != "" {
					fmt.Println(report)
				}

			case <-ctx.Done():
				fmt.Println("Interrupt!")
				firehose.CloseConnection()
				shutdown = true
			}
		}

		return nil
	},
}
