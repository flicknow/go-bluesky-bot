package blueskybot

import (
	"fmt"
	"strings"
	"time"

	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/dbx"
	cli "github.com/urfave/cli/v2"
)

var RequeueSelect = `
SELECT * FROM custom_labels
WHERE label_id = ?
AND created_at > ?
AND created_at < ?
AND neg = 1
ORDER BY custom_label_id ASC
LIMIT ?
`

var RequeueCmd = &cli.Command{
	Name:  "requeue",
	Flags: cmd.WithDb,
	Action: func(cctx *cli.Context) error {
		d := dbx.NewDBx(cmd.ToContext(cctx))
		bday, err := d.Labels.FindOrCreateLabel("birthday")
		if err != nil {
			return err
		}

		now := time.Now()
		start := now.Add(-1 * 24 * time.Hour).Unix()
		end := now.Add(-1 * time.Minute).Unix()

		count := 0
		chunk := 100
		for {
			labels := make([]*dbx.CustomLabel, 0, chunk)
			err := d.CustomLabels.Select(&labels, RequeueSelect, bday.LabelId, start, end, chunk)
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
