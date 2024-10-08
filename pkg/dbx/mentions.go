package dbx

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/jmoiron/sqlx"
)

type MentionRow struct {
	MentionId int64 `db:"mention_id"`
	PostId    int64 `db:"post_id"`
	ActorId   int64 `db:"actor_id"`
	SubjectId int64 `db:"subject_id"`
}

type DBxTableMentions struct {
	*sqlx.DB        `dbx-table:"mentions" dbx-pk:"mention_id"`
	path            string
	NamedStatements map[string]*sqlx.NamedStmt
	Statements      map[string]*sqlx.Stmt
}

var MentionSchema = `
CREATE TABLE IF NOT EXISTS mentions (
	mention_id INTEGER PRIMARY KEY,
	post_id INTEGER NOT NULL,
	actor_id INTEGER NOT NULL,
	subject_id INTEGER NOT NULL,
	UNIQUE(subject_id, post_id DESC) ON CONFLICT IGNORE
);
CREATE INDEX IF NOT EXISTS idx_mentions_post_id
ON mentions(post_id);
CREATE INDEX IF NOT EXISTS idx_actor_mentions_post_id
ON mentions(actor_id, post_id DESC);
`

func NewMentionTable(dir string) *DBxTableMentions {
	path := filepath.Join(dir, "mentions.db")
	return &DBxTableMentions{
		SQLxMustOpen(path, MentionSchema),
		path,
		make(map[string]*sqlx.NamedStmt),
		make(map[string]*sqlx.Stmt),
	}
}

func (d *DBxTableMentions) findOrPrepareNamedStmt(q string) (*sqlx.NamedStmt, error) {
	stmt := d.NamedStatements[q]
	if stmt != nil {
		return stmt, nil
	}

	var err error
	stmt, err = d.PrepareNamed(q)
	if err != nil {
		return nil, err
	}

	d.NamedStatements[q] = stmt
	return stmt, err
}
func (d *DBxTableMentions) findOrPrepareStmt(q string) (*sqlx.Stmt, error) {
	stmt := d.Statements[q]
	if stmt != nil {
		return stmt, nil
	}

	var err error
	stmt, err = d.Preparex(q)
	if err != nil {
		return nil, err
	}

	d.Statements[q] = stmt
	return stmt, err
}

func (d *DBxTableMentions) SelectMentions(postid int64) ([]int64, error) {
	var mentions []int64 = make([]int64, 0)
	err := d.Select(&mentions, "SELECT subject_id FROM mentions WHERE post_id = $1 ORDER BY subject_id ASC", postid)
	if err != nil {
		return nil, err
	}
	return mentions, nil
}

func (d *DBxTableMentions) SelectMentionsActorId(actorid int64, before int64, limit int) ([]int64, error) {
	q := `
SELECT
	post_id
FROM
	mentions
WHERE
	subject_id = $1
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

func (d *DBxTableMentions) InsertMentions(postid int64, actorid int64, mentionedActorIds []int64) error {
	if len(mentionedActorIds) == 0 {
		return nil
	}

	plcs := make([]string, len(mentionedActorIds))
	values := make([]any, 0, 3*len(mentionedActorIds))
	for i, mentionedActorId := range mentionedActorIds {
		plcs[i] = "(?, ?, ?)"
		values = append(values, postid, actorid, mentionedActorId)
	}

	q := fmt.Sprintf("INSERT INTO mentions (post_id, actor_id, subject_id) VALUES %s", strings.Join(plcs, ","))
	stmt, err := d.findOrPrepareStmt(q)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(values...)

	return err
}

func (d *DBxTableMentions) DeleteMentionByPostId(postid int64) error {
	_, err := d.Exec("DELETE FROM mentions WHERE post_id = $1", postid)
	return err
}
