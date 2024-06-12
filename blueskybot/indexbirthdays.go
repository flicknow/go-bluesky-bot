package blueskybot

import (
	"fmt"
	"os/signal"
	"syscall"

	"github.com/flicknow/go-bluesky-bot/pkg/client"
	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/indexer"
	cli "github.com/urfave/cli/v2"
)

var IndexBirthdaysCmd = &cli.Command{
	Name: "index-birthdays",
	Flags: cmd.CombineFlags(
		cmd.WithClient,
		cmd.WithDb,
		cmd.WithIndexer,
		&cli.Int64Flag{
			Name:  "limit",
			Usage: "number of birthdays to index",
			Value: 0,
		},
	),
	Action: func(cctx *cli.Context) error {
		ctx, stop := signal.NotifyContext(cmd.ToContext(cctx), syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
		defer stop()

		var cli client.Client = nil
		var err error = nil
		if cctx.Args().Len() != 0 {
			cli, err = client.NewClient(cctx)
			if err != nil {
				return err
			}
		} else {
			cli = client.NewMockClient(cmd.ToContext(cctx))
		}

		i, err := indexer.NewIndexer(ctx, cli)
		if err != nil {
			return err
		}

		shutdown := false
		go func() {
			<-ctx.Done()
			shutdown = true
		}()

		count := int64(0)
		cutoff := int64(0)
		limit := cctx.Int64("limit")
		for !shutdown {
			actors, err := i.InitializeActorBirthdaysOnce(cutoff, 100)
			if err != nil {
				return err
			}
			if len(actors) < 100 {
				return nil
			}
			cutoff = actors[len(actors)-1].ActorId

			count += int64(len(actors))
			fmt.Printf("> indexed %d birthdays!\n", count)
			if limit > 0 {
				if count >= limit {
					return nil
				}
			}
		}

		return nil
	},
}
