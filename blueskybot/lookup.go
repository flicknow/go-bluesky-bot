package blueskybot

import (
	"fmt"
	"strings"

	cli "github.com/urfave/cli/v2"
)

var LookupCmd = &cli.Command{
	Name:  "lookup",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		for _, uri := range cctx.Args().Slice() {
			parts := strings.SplitN(uri[5:], "/", 3)

			did, err := lookupDid(parts[0])
			if err != nil {
				return err
			}

			record, err := lookupPlcDirectoryRecord(did)
			if err != nil {
				return err
			}

			if (record.AlsoKnownAs == nil) || (len(record.AlsoKnownAs) == 0) {
				return fmt.Errorf("could not find handle from record %+v", record)
			}

			fmt.Printf("https://bsky.app/profile/%s/post/%s\n", record.AlsoKnownAs[0][5:], parts[2])
		}

		return nil
	},
}
