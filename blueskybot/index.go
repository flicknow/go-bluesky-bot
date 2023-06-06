package blueskybot

import (
	"fmt"
	"strings"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/flicknow/go-bluesky-bot/pkg/client"
	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/firehose"
	"github.com/flicknow/go-bluesky-bot/pkg/indexer"
	"github.com/flicknow/go-bluesky-bot/pkg/utils"
	cli "github.com/urfave/cli/v2"
)

var IndexCmd = &cli.Command{
	Name: "index",
	Flags: cmd.CombineFlags(
		cmd.WithClient,
		cmd.WithIndexer,
		&cli.IntFlag{
			Name:  "actors",
			Usage: "index uninitialized actors",
			Value: 0,
		},
	),
	Action: func(cctx *cli.Context) error {
		client, err := client.NewClient(cctx)
		if err != nil {
			return err
		}

		indexer, err := indexer.NewIndexer(cmd.ToContext(cctx), client)
		if actorsCount := cctx.Int("actors"); actorsCount != 0 {
			actors, _, err := indexer.InitUninitializedActors(600, 0, actorsCount)
			if err != nil {
				return nil
			}
			fmt.Printf("INDEXED %d actors\n", len(actors))
			return nil
		}

		for _, uri := range cctx.Args().Slice() {
			parts := strings.SplitN(uri[5:], "/", 3)
			if len(parts) != 3 {
				return fmt.Errorf("cannot parse uri %s: expected to found 2 /", uri)
			}

			switch parts[1] {
			case "app.bsky.feed.post":
				postviews, err := client.GetPosts([]string{uri})
				if err != nil {
					return err
				}
				if len(postviews) != 1 {
					return fmt.Errorf("expected find 1 post for %s, found %d", uri, len(postviews))
				}
				postview := postviews[0]
				ref := &comatproto.RepoStrongRef{
					LexiconTypeID: postview.LexiconTypeID,
					Cid:           postview.Cid,
					Uri:           postview.Uri,
				}

				post := &appbsky.FeedPost{}
				err = utils.DecodeCBOR(postview.Record.Val, &post)
				if err != nil {
					return err
				}

				postref := firehose.NewPostRef(post, ref, 0)
				_, err = indexer.Post(postref)
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("unhandled type: %s", parts[1])
			}
		}
		return nil
	},
}
