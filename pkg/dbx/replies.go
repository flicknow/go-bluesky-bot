package dbx

import (
	"fmt"
	"path/filepath"

	"github.com/jmoiron/sqlx"
)

type ReplyRow struct {
	ReplyId       int64 `db:"reply_id"`
	PostId        int64 `db:"post_id"`
	ActorId       int64 `db:"actor_id"`
	ParentId      int64 `db:"parent_id"`
	ParentActorId int64 `db:"parent_actor_id"`
	RootId        int64 `db:"root_id"`
	RootActorId   int64 `db:"root_actor_id"`
}

type DBxTableReplies struct {
	*sqlx.DB `dbx-table:"replies" dbx-pk:"reply_id"`
	path     string
}

var ReplySchema = `
CREATE TABLE IF NOT EXISTS replies (
	reply_id INTEGER PRIMARY KEY,
	post_id INTEGER NOT NULL,
	actor_id INTEGER NOT NULL,
	parent_id INTEGER NOT NULL,
	parent_actor_id INTEGER NOT NULL,
	root_id INTEGER NOT NULL,
	root_actor_id INTEGER NOT NULL,
	UNIQUE(post_id) ON CONFLICT IGNORE
);
CREATE INDEX IF NOT EXISTS idx_replies_parent_actor_id
ON replies(parent_actor_id, post_id DESC);
CREATE INDEX IF NOT EXISTS idx_replies_parent_actor_actor_id
ON replies(parent_actor_id, actor_id, post_id DESC);
`

func NewReplyTable(dir string) *DBxTableReplies {
	path := filepath.Join(dir, "replies.db")
	return &DBxTableReplies{
		SQLxMustOpen(path, ReplySchema),
		path,
	}
}

func (d *DBxTableReplies) FindByPostId(postid int64) (*ReplyRow, error) {
	row := d.QueryRowx("SELECT * FROM replies WHERE post_id = ?", postid)
	if row == nil {
		return nil, fmt.Errorf("could not find reply metadata for post id %d", postid)
	}

	reply := &ReplyRow{}
	err := row.StructScan(reply)
	if err != nil {
		return nil, err
	}

	return reply, nil
}

func (d *DBxTableReplies) SelectRepliesByActorId(actorid int64, before int64, limit int) ([]int64, error) {
	q := `
SELECT
	post_id
FROM
	replies
WHERE
	parent_actor_id = $1
	AND actor_id != $1
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

func (d *DBxTableReplies) InsertReply(r *ReplyRow) error {
	_, err := d.NamedExec("INSERT INTO replies (post_id, actor_id, parent_id, parent_actor_id, root_id, root_actor_id) VALUES (:post_id, :actor_id, :parent_id, :parent_actor_id, :root_id, :root_actor_id)", r)
	return err
}

func (d *DBxTableReplies) DeleteReply(replyid int64) error {
	_, err := d.Exec("DELETE FROM replies WHERE reply_id = $1", replyid)
	return err
}
