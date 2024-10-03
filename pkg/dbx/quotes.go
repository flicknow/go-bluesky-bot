package dbx

import (
	"fmt"
	"path/filepath"

	"github.com/jmoiron/sqlx"
)

type QuoteRow struct {
	QuoteId        int64 `db:"quote_id"`
	PostId         int64 `db:"post_id"`
	ActorId        int64 `db:"actor_id"`
	SubjectId      int64 `db:"subject_id"`
	SubjectActorId int64 `db:"subject_actor_id"`
}

type DBxTableQuotes struct {
	*sqlx.DB        `dbx-table:"quotes" dbx-pk:"quote_id"`
	path            string
	NamedStatements map[string]*sqlx.NamedStmt
	Statements      map[string]*sqlx.Stmt
}

var QuoteSchema = `
CREATE TABLE IF NOT EXISTS quotes (
	quote_id INTEGER PRIMARY KEY,
	post_id INTEGER NOT NULL,
	actor_id INTEGER NOT NULL,
	subject_id INTEGER NOT NULL,
	subject_actor_id INTEGER NOT NULL,
	UNIQUE(post_id) ON CONFLICT IGNORE
);
CREATE INDEX IF NOT EXISTS idx_quotes_subject_id
ON quotes(subject_id);
CREATE INDEX IF NOT EXISTS idx_quotes_subject_actor_id
ON quotes(subject_actor_id, post_id DESC);
CREATE INDEX IF NOT EXISTS idx_quotes_subject_actor_actor_id
ON quotes(subject_actor_id, actor_id, post_id DESC);
`

func NewQuoteTable(dir string) *DBxTableQuotes {
	path := filepath.Join(dir, "quotes.db")
	return &DBxTableQuotes{
		SQLxMustOpen(path, QuoteSchema),
		path,
		make(map[string]*sqlx.NamedStmt),
		make(map[string]*sqlx.Stmt),
	}
}
func (d *DBxTableQuotes) findOrPrepareNamedStmt(q string) (*sqlx.NamedStmt, error) {
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
func (d *DBxTableQuotes) findOrPrepareStmt(q string) (*sqlx.Stmt, error) {
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

func (d *DBxTableQuotes) FindByPostId(postid int64) (*QuoteRow, error) {
	row := d.QueryRowx("SELECT * FROM quotes WHERE post_id = ?", postid)
	if row == nil {
		return nil, fmt.Errorf("could not find quote metadata for post id %d", postid)
	}

	quote := &QuoteRow{}
	err := row.StructScan(quote)
	if err != nil {
		return nil, err
	}

	return quote, nil
}

func (d *DBxTableQuotes) SelectQuotesBySubjectId(subjectid int64, before int64, limit int) ([]int64, error) {
	q := `
SELECT
	post_id
FROM
	quotes
WHERE
	subject_id = $1
	AND post_id < $2
ORDER BY
	post_id DESC
LIMIT
	$3
`

	quotes := make([]int64, 0, limit)
	rows, err := d.Queryx(q, subjectid, before, limit)
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
		quotes = append(quotes, mention.PostId)
	}

	return quotes, nil
}

func (d *DBxTableQuotes) SelectQuotesByActorId(actorid int64, before int64, limit int) ([]int64, error) {
	q := `
SELECT
	post_id
FROM
	quotes
WHERE
	subject_actor_id = $1
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

func (d *DBxTableQuotes) InsertQuote(q *QuoteRow) error {
	stmt, err := d.findOrPrepareNamedStmt("INSERT INTO quotes (post_id, actor_id, subject_id, subject_actor_id) VALUES (:post_id, :actor_id, :subject_id, :subject_actor_id)")
	if err != nil {
		return err
	}

	_, err = stmt.Exec(q)
	return err
}

func (d *DBxTableQuotes) DeleteQuote(quoteid int64) error {
	_, err := d.Exec("DELETE FROM quotes WHERE quote_id = $1", quoteid)
	return err
}
