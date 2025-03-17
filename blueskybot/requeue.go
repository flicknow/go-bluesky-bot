package blueskybot

import (
	"fmt"
	"strings"
	"time"

	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/dbx"
	cli "github.com/urfave/cli/v2"
)

var CountSelect = `
SELECT count(*) FROM custom_labels
WHERE created_at > ?
AND created_at < ?
ORDER BY custom_label_id ASC
`

var RequeueSelect = `
SELECT * FROM custom_labels
WHERE created_at > ?
AND created_at < ?
ORDER BY custom_label_id ASC
LIMIT ?
`

var RequeueCmd = &cli.Command{
	Name: "requeue",
	Flags: cmd.CombineFlags(
		cmd.WithDb,
		&cli.DurationFlag{
			Name:  "since",
			Usage: "time period since to requeue",
			Value: 24 * time.Hour,
		},
		&cli.BoolFlag{
			Name:  "dry-run",
			Usage: "do not requeue labels",
			Value: false,
		},
	),
	Action: func(cctx *cli.Context) error {
		d := dbx.NewDBx(cmd.ToContext(cctx))

		dryrun := cctx.Bool("dry-run")
		since := cctx.Duration("since")

		now := time.Now()
		start := now.Add(-1 * since).Unix()
		end := now.Add(-1 * time.Minute).Unix()

		if dryrun {
			var count int64 = 0
			row := d.CustomLabels.QueryRow(CountSelect, start, end)
			err := row.Scan(&count)
			if err != nil {
				return err
			}
			fmt.Printf("> would have requeued %d labels\n", count)
			return nil
		}

		count := 0
		chunk := 100
		for {
			labels := make([]*dbx.CustomLabel, 0, chunk)
			err := d.CustomLabels.Select(&labels, RequeueSelect, start, end, chunk)
			if err != nil {
				return err
			}
			if len(labels) == 0 {
				break
			}
			count += len(labels)

			fmt.Printf("> requeueing %d labels: %d\n", count, labels[0].SubjectId)

			tx, err := d.CustomLabels.Beginx()
			if err != nil {
				return err
			}
			defer tx.Rollback()

			ids := make([]string, len(labels))
			for i, label := range labels {
				ids[i] = fmt.Sprintf("%d", label.CustomLabelId)
			}

			deleteSql := fmt.Sprintf("DELETE FROM custom_labels WHERE custom_label_id IN (%s)", strings.Join(ids, ","))
			_, err = tx.Exec(deleteSql)
			if err != nil {
				return err
			}

			for _, label := range labels {
				label.CreatedAt = now.Unix()
				_, err := tx.NamedExec(
					"INSERT OR IGNORE INTO custom_labels (label_id, created_at, neg, subject_type, subject_id, cbor) VALUES (:label_id, :created_at, :neg, :subject_type, :subject_id, :cbor)",
					label,
				)
				if err != nil {
					return err
				}
			}

			err = tx.Commit()
			if err != nil {
				return err
			}
		}

		return nil
	},
}
