package dbx

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/jmoiron/sqlx"
)

type ThreadMentionRow struct {
	ThreadMentionId int64 `db:"thread_mention_id"`
	PostId          int64 `db:"post_id"`
	ActorId         int64 `db:"actor_id"`
}

type DBxTableThreadMentions struct {
	*sqlx.DB `dbx-table:"thread_mentions" dbx-pk:"thread_mention_id"`
	path     string
}

var ThreadMentionSchema = `
CREATE TABLE IF NOT EXISTS thread_mentions (
	thread_mention_id INTEGER PRIMARY KEY,
	post_id INTEGER NOT NULL,
	actor_id INTEGER NOT NULL,
	UNIQUE(actor_id, post_id DESC) ON CONFLICT IGNORE
);
CREATE INDEX IF NOT EXISTS idx_thread_mention_post_id
ON thread_mentions(post_id);
`

func NewThreadMentionTable(dir string) *DBxTableThreadMentions {
	path := filepath.Join(dir, "thread-mentions.db")
	return &DBxTableThreadMentions{
		SQLxMustOpen(path, ThreadMentionSchema),
		path,
	}
}

func (d *DBxTableThreadMentions) SelectThreadMentions(postid int64) ([]int64, error) {
	var mentions []int64 = make([]int64, 0)
	err := d.Select(&mentions, "SELECT actor_id FROM thread_mentions WHERE post_id = $1 ORDER BY actor_id ASC", postid)
	if err != nil {
		return nil, err
	}
	return mentions, nil
}

func (d *DBxTableThreadMentions) SelectThreadMentionsByActorId(actorid int64, before int64, limit int) ([]int64, error) {
	q := `
SELECT
	post_id
FROM
	thread_mentions
WHERE
	actor_id = $1
	AND post_id < $2
ORDER BY
	post_id DESC
LIMIT
	$3
`

	mentions := make([]int64, 0, limit)
	rows, err := d.Queryx(q, actorid, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		mention := &ThreadMentionRow{}
		err = rows.StructScan(&mention)
		if err != nil {
			return nil, err
		}
		mentions = append(mentions, mention.PostId)
	}

	return mentions, nil
}

func (d *DBxTableThreadMentions) InsertThreadMention(postid int64, actorids []int64) error {
	if len(actorids) == 0 {
		return nil
	}

	values := make([]string, len(actorids))
	for i, actorid := range actorids {
		values[i] = fmt.Sprintf("(%d, %d)", postid, actorid)
	}

	q := fmt.Sprintf("INSERT INTO thread_mentions (post_id, actor_id) VALUES %s", strings.Join(values, ","))
	_, err := d.Exec(q)

	return err
}

func (d *DBxTableThreadMentions) DeleteThreadMentionsByPostId(postid int64) error {
	_, err := d.Exec("DELETE FROM thread_mentions WHERE post_id = $1", postid)
	return err
}
