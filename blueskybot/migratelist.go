package blueskybot

import (
	"fmt"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/flicknow/go-bluesky-bot/pkg/client"
	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/ticker"
	cli "github.com/urfave/cli/v2"
)

var MigrateListCmd = &cli.Command{
	Name:      "migrate-list",
	ArgsUsage: "FROM TO",
	Flags: cmd.CombineFlags(
		cmd.WithClient,
	),
	Action: func(cctx *cli.Context) error {
		args := cctx.Args().Slice()
		if len(args) != 2 {
			return cli.ShowSubcommandHelp(cctx)
		}

		cli, err := client.NewClient(cctx)
		if err != nil {
			return err
		}

		sourcelist := args[0]
		cursor := ""
		dids := []string{}
		for {
			var items []*bsky.GraphDefs_ListItemView = nil
			items, cursor, err = cli.GetList(sourcelist, cursor, 100)
			if err != nil {
				return err
			}
			for _, item := range items {
				dids = append(dids, item.Subject.Did)
			}

			if (cursor == "") || (items == nil) || (len(items) == 0) {
				break
			}
		}

		dstlist := args[1]
		cursor = ""
		exists := make(map[string]bool)
		for {
			var items []*bsky.GraphDefs_ListItemView = nil
			items, cursor, err = cli.GetList(dstlist, cursor, 100)
			if err != nil {
				return err
			}
			for _, item := range items {
				exists[item.Subject.Did] = true
			}

			if (cursor == "") || (items == nil) || (len(items) == 0) {
				break
			}
		}
		fmt.Printf("found %d actors in %s\n", len(exists), dstlist)

		fmt.Println("compiled source list")
		throttle := ticker.NewTicker(500 * time.Millisecond)
		for _, actor := range dids {
			if exists[actor] {
				continue
			}

			<-throttle.C

			fmt.Printf("> appending %s to %s\n", actor, dstlist)
			res, err := cli.AppendList(dstlist, actor)
			if err != nil {
				return err
			}
			fmt.Printf("res=%#v\n", res)
		}

		return nil
	},
}
