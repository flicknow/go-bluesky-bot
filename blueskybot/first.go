package blueskybot

import (
	"fmt"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/flicknow/go-bluesky-bot/pkg/client"
	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	cli "github.com/urfave/cli/v2"
)

var FirstCmd = &cli.Command{
	Name:  "first",
	Flags: cmd.WithClient,
	Action: func(cctx *cli.Context) error {
		client, err := client.NewClient(cctx)
		if err != nil {
			return err
		}

		max := int64(100 * 1000)
		for _, uri := range cctx.Args().Slice() {
			actor, err := client.GetActor(uri)
			if err != nil {
				return err
			}
			if actor.PostsCount == nil {
				return fmt.Errorf("could not get post count for %s", uri)
			}
			if *actor.PostsCount > max {
				return fmt.Errorf("actor %s has too many posts %d. limit %d", uri, *actor.PostsCount, max)
			}

			cursor := ""
			done := false
			var firstPost *bsky.FeedDefs_PostView = nil
			var firstOp *bsky.FeedDefs_PostView = nil
			for !done {
				views, c, err := client.GetAuthorFeed(actor.Did, "posts_no_replies", 100, cursor)
				if err != nil {
					return err
				}

				for _, view := range views {
					firstPost = view.Post
					if (view.Reply == nil) || (view.Reply.Parent == nil) {
						firstOp = view.Post
					}
				}

				cursor = c
				if cursor == "" {
					done = true
					break
				}

				fmt.Println(cursor)
			}

			if firstPost != nil {
				fmt.Printf("first post: %s\n", firstPost.Uri)
			}
			if firstOp != nil {
				fmt.Printf("first op: %s\n", firstOp.Uri)

			}

			return nil
		}

		return nil
	},
}
