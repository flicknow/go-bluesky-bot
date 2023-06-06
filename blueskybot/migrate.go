package blueskybot

import (
	"fmt"
	"os"

	"github.com/jmoiron/sqlx"
	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/dbx"
	cli "github.com/urfave/cli/v2"
)

var MigrateCmd = &cli.Command{
	Name: "migrate",
	Flags: cmd.CombineFlags(
		cmd.WithDb,
		&cli.StringFlag{
			Name:    "db",
			Usage:   "db path",
			Value:   fmt.Sprintf("%s/.bsky.index.db", os.Getenv("HOME")),
			EnvVars: []string{"GO_BLUESKY_DB"},
		},
	),
	Action: func(cctx *cli.Context) error {
		d := dbx.NewDBx(cmd.ToContext(cctx))
		legacyPath := cctx.String("db")
		defer d.Close()

		migrateActors(attachLegacyDb(d.Actors.DB, legacyPath))
		migrateDms(attachLegacyDb(d.Dms.DB, legacyPath))
		migrateLabels(attachLegacyDb(d.Labels.DB, legacyPath))
		migrateMentions(attachLegacyDb(d.Mentions.DB, legacyPath))
		migratePosts(attachLegacyDb(d.Posts.DB, legacyPath))
		migratePostLabels(attachLegacyDb(d.PostLabels.DB, legacyPath))
		migrateQuotes(attachLegacyDb(d.Quotes.DB, legacyPath))
		migrateReplies(attachLegacyDb(d.Replies.DB, legacyPath))
		migrateThreadMentions(attachLegacyDb(d.ThreadMentions.DB, legacyPath))

		return nil
	},
}

func attachLegacyDb(d *sqlx.DB, legacyPath string) *sqlx.DB {
	_, err := d.Exec(
		fmt.Sprintf("ATTACH DATABASE \"%s\" AS legacy", legacyPath),
	)
	if err != nil {
		panic(err)
	}
	return d
}

func migrateActors(d *sqlx.DB) {
	_, err := d.Exec("INSERT INTO actors SELECT * FROM legacy.actors ORDER BY actor_id ASC")
	if err != nil {
		panic(err)
	}
}

func migrateDms(d *sqlx.DB) {
	_, err := d.Exec("INSERT INTO dms SELECT * FROM legacy.dms ORDER BY dm_id ASC")
	if err != nil {
		panic(err)
	}
}

func migrateLabels(d *sqlx.DB) {
	_, err := d.Exec("INSERT INTO labels SELECT * FROM legacy.labels ORDER BY label_id ASC")
	if err != nil {
		panic(err)
	}
}

func migrateMentions(d *sqlx.DB) {
	_, err := d.Exec("INSERT INTO mentions (mention_id, post_id, actor_id, subject_id) SELECT m.mention_id, m.post_id, p.actor_id, m.actor_id FROM legacy.mentions m JOIN legacy.posts p USING (post_id) ORDER BY mention_id ASC")
	if err != nil {
		panic(err)
	}
}
func migratePostLabels(d *sqlx.DB) {
	_, err := d.Exec("INSERT INTO post_labels SELECT * FROM legacy.post_labels ORDER BY post_label_id ASC")
	if err != nil {
		panic(err)
	}
}
func migratePosts(d *sqlx.DB) {
	_, err := d.Exec("INSERT INTO posts SELECT * FROM legacy.posts ORDER BY post_id ASC")
	if err != nil {
		panic(err)
	}

}
func migrateQuotes(d *sqlx.DB) {
	_, err := d.Exec("INSERT INTO quotes (quote_id, post_id, actor_id, subject_id, subject_actor_id) SELECT quote_id, post_id, actor_id, subject_id, subject_actor_id FROM legacy.quotes JOIN legacy.posts USING (post_id) ORDER BY quote_id ASC")
	if err != nil {
		panic(err)
	}
}
func migrateReplies(d *sqlx.DB) {
	_, err := d.Exec("INSERT INTO replies (reply_id, post_id, actor_id, parent_id, parent_actor_id, root_id, root_actor_id) SELECT reply_id, post_id, actor_id, parent_id, parent_actor_id, root_id, root_actor_id FROM legacy.replies JOIN legacy.posts USING (post_id) ORDER BY reply_id ASC")
	if err != nil {
		panic(err)
	}
}
func migrateThreadMentions(d *sqlx.DB) {
	_, err := d.Exec("INSERT INTO thread_mentions SELECT * FROM legacy.thread_mentions ORDER BY thread_mention_id ASC")
	if err != nil {
		panic(err)
	}
}
