package blueskybot

import (
	"fmt"
	"log"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/flicknow/go-bluesky-bot/pkg/client"
	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/utils"
	cli "github.com/urfave/cli/v2"
)

var DumpCmd = &cli.Command{
	Name: "dump",
	Flags: cmd.CombineFlags(
		cmd.WithClient,
	),
	Action: func(cctx *cli.Context) error {
		cli, err := client.NewClient(cctx)
		if err != nil {
			return err
		}

		posts, err := cli.GetPosts(cctx.Args().Slice())
		if err != nil {
			return err
		}

		for _, p := range posts {
			fmt.Printf("appview post = %#v\n", p.Embed)

			rec, err := cli.GetRecord(p.Uri, p.Cid)
			if err != nil {
				return err
			}
			fmt.Printf("record = %#v\n", rec.Value.Val)

			embed, ok := rec.Value.Val.(*appbsky.FeedPost)
			if ok {
				fmt.Printf("embed = %#v\n", embed.Embed)
			}

			bytes, err := rec.Value.MarshalJSON()
			if err == nil {
				fmt.Printf("record = %s\n", string(bytes))
			}

			post := &appbsky.FeedPost{}
			err = utils.DecodeCBOR(rec.Value.Val, &post)
			if err != nil {
				log.Printf("error decoding %s: %+v", p.Uri, err)
				fmt.Printf("\n%%T=%T\n%%+v=%+v\n%%#v=%#v\n", rec.Value.Val, rec.Value.Val, rec.Value.Val)
				return nil
			}
			fmt.Printf("decoded post = %+v\n", post)
		}

		return nil
	},
}
