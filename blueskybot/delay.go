package blueskybot

import (
	"fmt"

	"github.com/flicknow/go-bluesky-bot/pkg/clock"
	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/dbx"
	cli "github.com/urfave/cli/v2"
)

var DelayCmd = &cli.Command{
	Name: "delay",
	Flags: cmd.CombineFlags(
		cmd.WithDb,
		&cli.IntFlag{
			Name:  "chunk",
			Usage: "chunk size",
			Value: 30,
		},
	),
	Action: func(cctx *cli.Context) error {
		db := dbx.NewDBx(cmd.ToContext(cctx))

		now := clock.NewClock().NowUnix()
		posts, err := db.SelectLatestPosts(now, 1)
		if err != nil {
			return err
		}

		if (posts == nil) || (len(posts) == 0) {
			return nil
		}

		fmt.Printf("%d\n", now-posts[0].CreatedAt)

		return nil
	},
}
