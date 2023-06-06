package blueskybot

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/flicknow/go-bluesky-bot/pkg/client"
	"github.com/flicknow/go-bluesky-bot/pkg/clock"
	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/indexer"
	"github.com/flicknow/go-bluesky-bot/pkg/sleeper"
	cli "github.com/urfave/cli/v2"
)

var PruneCmd = &cli.Command{
	Name: "prune",
	Flags: cmd.CombineFlags(
		cmd.WithClient,
		cmd.WithIndexer,
		&cli.IntFlag{
			Name:  "chunk",
			Usage: "chunk size",
			Value: 30,
		},
		&cli.Int64Flag{
			Name:  "pause",
			Usage: "number of milliseconds to pause between prunes",
			Value: 250,
		},
		&cli.IntFlag{
			Name:  "limit",
			Usage: "prune limit",
			Value: 1000,
		},
	),
	Action: func(cctx *cli.Context) error {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
		defer stop()

		sleeper := sleeper.NewSleeper(ctx)
		shutdown := false
		go func() {
			<-ctx.Done()
			shutdown = true
		}()

		client, err := client.NewClient(cctx)
		if err != nil {
			return err
		}

		indexer, err := indexer.NewIndexer(cmd.ToContext(cctx), client)
		if err != nil {
			return err
		}

		since := clock.NewClock().NowUnix() - (cctx.Int64("keep-days") * 24 * 60 * 60)
		chunk := cctx.Int("chunk")
		limit := cctx.Int("limit")
		pause := cctx.Int64("pause")
		total := 0

	PRUNE:
		for {
			if shutdown {
				break PRUNE
			}

			c := chunk
			if (total + chunk) >= limit {
				c = limit - total
			}

			pruned, err := indexer.Db.Prune(since, c)
			if err != nil {
				return err
			}

			total += pruned
			if (pruned == 0) || (total >= limit) {
				break PRUNE
			}

			sleeper.Sleep(time.Duration(pause) * time.Millisecond)
		}

		fmt.Printf("> prune %d posts\n", total)

		return nil
	},
}
