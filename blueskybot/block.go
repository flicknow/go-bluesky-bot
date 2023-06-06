package blueskybot

import (
	"fmt"
	"strings"

	"github.com/flicknow/go-bluesky-bot/pkg/client"
	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/dbx"
	cli "github.com/urfave/cli/v2"
)

var BlockCmd = &cli.Command{
	Name: "block",
	Flags: cmd.CombineFlags(
		cmd.WithClient,
		cmd.WithDb,
	),
	Action: func(cctx *cli.Context) error {
		client, err := client.NewClient(cctx)
		if err != nil {
			return err
		}

		db := dbx.NewDBx(cmd.ToContext(cctx))

		for _, did := range cctx.Args().Slice() {
			if !strings.Contains(did, "did:") {
				actor, err := client.GetActor(did)
				if err != nil {
					return err
				}
				did = actor.Did
			}

			actor, err := db.Actors.FindOrCreateActor(did)
			if err != nil {
				return err
			}
			if actor == nil {
				return fmt.Errorf("could not find indexed actor %s", did)
			}

			actor.Blocked = true
			err = db.InitActorInfo(actor, []*dbx.PostLabelRow{})
			if err != nil {
				return err
			}
		}

		return nil
	},
}
