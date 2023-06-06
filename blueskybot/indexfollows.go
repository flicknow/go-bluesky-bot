package blueskybot

import (
	"fmt"
	"os/signal"
	"strings"
	"syscall"

	"github.com/flicknow/go-bluesky-bot/pkg/client"
	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/dbx"
	"github.com/flicknow/go-bluesky-bot/pkg/indexer"
	cli "github.com/urfave/cli/v2"
)

var IndexFollowsCmd = &cli.Command{
	Name: "index-follows",
	Flags: cmd.CombineFlags(
		cmd.WithClient,
		cmd.WithDb,
		cmd.WithIndexer,
		&cli.Int64Flag{
			Name:  "from-unindexed",
			Usage: "number of unindexed actors to index",
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

		d := i.Db
		for _, handle := range cctx.Args().Slice() {
			did, err := lookupDid(handle)
			if err != nil {
				return err
			}

			actor, err := d.Actors.FindOrCreateActor(did)
			if err != nil {
				return err
			}

			c, err := i.IndexFollows(600, actor)
			if err != nil {
				return err
			}
			fmt.Printf("> indexed %d follows for %s\n", c, handle)
		}

		fromUnindexed := cctx.Int64("from-unindexed")
		if fromUnindexed != 0 {
			limit := 100
			if limit > int(fromUnindexed) {
				limit = int(fromUnindexed)
			}

			var last int64 = 0
			var seen int64 = 0

			actorids := []string{}
			d.FollowsIndexed.Select(&actorids, "SELECT actor_id FROM follows_indexed ORDER BY follow_indexed_id ASC")
			indexed := strings.Join(actorids, ",")

			for seen < fromUnindexed {
				actors := make([]*dbx.ActorRow, 0, limit)
				err := d.Actors.Select(&actors, fmt.Sprintf("SELECT * FROM actors WHERE actor_id > $1 AND blocked = 0 AND created_at > 0 AND actor_id NOT IN (%s) ORDER BY actor_id ASC LIMIT $2", indexed), last, limit)
				if err != nil {
					return nil
				}
				if len(actors) < limit {
					break
				}

				for _, actor := range actors {
					indexed, err := d.FollowsIndexed.FindOrCreateByActorId(actor.ActorId)
					if err != nil {
						return err
					}
					if indexed.Created {
						seen++
					}

					last = actor.ActorId
				}
			}

			fmt.Printf("> marked %d actors for follow indexing\n", seen)
		}

		return nil
	},
}
