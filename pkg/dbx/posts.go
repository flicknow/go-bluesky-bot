package dbx

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/flicknow/go-bluesky-bot/pkg/utils"
	"github.com/jmoiron/sqlx"
)

type PostRow struct {
	PostId        int64  `db:"post_id"`
	DehydratedUri string `db:"uri"`
	Uri           string
	ActorId       int64 `db:"actor_id"`
	CreatedAt     int64 `db:"created_at"`
	Labeled       int64 `db:"labeled"`
	Likes         int64 `db:"likes"`
	Quotes        int64 `db:"quotes"`
	Replies       int64 `db:"replies"`
	Reposts       int64 `db:"reposts"`
}

type DBxTablePosts struct {
	*sqlx.DB        `dbx-table:"posts" dbx-pk:"post_id"`
	Path            string
	NamedStatements map[string]*sqlx.NamedStmt
	Statements      map[string]*sqlx.Stmt
}

var PostSchema = `
CREATE TABLE IF NOT EXISTS posts (
	post_id INTEGER PRIMARY KEY,
	uri TEXT NOT NULL UNIQUE,
	actor_id INTEGER DEFAULT 0,
	created_at INTEGER NOT NULL,
	labeled INTEGER DEFAULT 0,
	likes INTEGER DEFAULT 0,
	quotes INTEGER DEFAULT 0,
	replies INTEGER DEFAULT 0,
	reposts INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_post_created_at
ON posts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_post_labeled
ON posts(labeled, post_id DESC);
`

func NewPostTable(dir string) *DBxTablePosts {
	path := filepath.Join(dir, "posts.db")
	return NewPostTableWithPath(path)
}

func NewPostTableWithPath(path string) *DBxTablePosts {
	table := &DBxTablePosts{
		SQLxMustOpen(path, PostSchema),
		path,
		make(map[string]*sqlx.NamedStmt),
		make(map[string]*sqlx.Stmt),
	}
	return table
}

func (d *DBxTablePosts) findOrPrepareNamedStmt(q string) (*sqlx.NamedStmt, error) {
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
func (d *DBxTablePosts) findOrPrepareStmt(q string) (*sqlx.Stmt, error) {
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

func (d *DBxTablePosts) FindByUris(uris []string) ([]*PostRow, error) {
	postrows := make([]*PostRow, 0, len(uris))

	plcs := make([]string, len(uris))
	params := make([]any, len(uris))
	for i, uri := range uris {
		plcs[i] = "?"
		params[i] = utils.DehydrateUri(uri)
	}

	q := fmt.Sprintf("SELECT * FROM posts WHERE uri IN (%s)", strings.Join(plcs, ","))
	stmt, err := d.findOrPrepareStmt(q)
	if err != nil {
		return nil, err
	}

	err = stmt.Select(&postrows, params...)
	if (err != nil) && errors.Is(err, sql.ErrNoRows) {
		return postrows, nil
	} else if err != nil {
		return nil, err
	}

	for _, post := range postrows {
		post.Uri = utils.HydrateUri(post.DehydratedUri, "app.bsky.feed.post")
	}

	return postrows, nil
}

func (d *DBxTablePosts) FindByUri(uri string) (*PostRow, error) {
	stmt, err := d.findOrPrepareStmt("SELECT * FROM posts WHERE uri = ?")
	if err != nil {
		return nil, err
	}

	postrow := &PostRow{}
	err = stmt.Get(postrow, utils.DehydrateUri(uri))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		} else {
			return nil, err
		}
	}
	postrow.Uri = utils.HydrateUri(postrow.DehydratedUri, "app.bsky.feed.post")
	return postrow, nil
}

func (d *DBxTablePosts) FindPostIdByUri(uri string) (int64, error) {
	stmt, err := d.findOrPrepareStmt("SELECT post_id FROM posts WHERE uri = ?")
	if err != nil {
		return 0, err
	}

	var postid int64 = 0
	err = stmt.Get(&postid, utils.DehydrateUri(uri))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		} else {
			return 0, err
		}
	}
	return postid, nil
}

func (d *DBxTablePosts) SelectPostsByActorId(actorid int64, before int64, limit int) ([]*PostRow, error) {
	stmt, err := d.findOrPrepareStmt("SELECT * FROM posts WHERE actor_id = $1 AND post_id < $2 ORDER BY post_id DESC LIMIT $3")
	if err != nil {
		return nil, err
	}

	postrows := make([]*PostRow, 0, limit)
	err = stmt.Select(&postrows, actorid, before, limit)
	if err != nil {
		return nil, err
	}

	for _, post := range postrows {
		post.Uri = utils.HydrateUri(post.DehydratedUri, "app.bsky.feed.post")

	}

	return postrows, nil
}

func (d *DBxTablePosts) SelectPostsByActorIds(actorids []int64, before int64, limit int) ([]*PostRow, error) {
	posts := make([]*PostRow, 0, len(actorids))

	if len(actorids) == 0 {
		return posts, nil
	}

	params := make([]any, 0, len(actorids)+2)
	params = append(params, before)

	plcs := make([]string, len(actorids))
	for i, actorid := range actorids {
		params = append(params, actorid)
		plcs[i] = "?"
	}
	params = append(params, limit)

	q := fmt.Sprintf("SELECT * FROM posts WHERE post_id < ? AND actor_id IN (%s) ORDER BY post_id DESC LIMIT ?", strings.Join(plcs, ", "))
	err := d.Select(&posts, q, params...)
	if err != nil {
		return nil, err
	}

	for _, post := range posts {
		post.Uri = utils.HydrateUri(post.DehydratedUri, "app.bsky.feed.post")

	}

	return posts, nil
}

func (d *DBxTablePosts) SelectPostsById(postids []int64) ([]*PostRow, error) {
	posts := make([]*PostRow, 0, len(postids))

	if len(postids) == 0 {
		return posts, nil
	}

	params := make([]any, len(postids))
	plcs := make([]string, len(postids))
	for i, uri := range postids {
		params[i] = uri
		plcs[i] = "?"
	}
	q := fmt.Sprintf("SELECT * FROM posts WHERE post_id IN (%s)", strings.Join(plcs, ", "))

	err := d.Select(&posts, q, params...)
	if err != nil {
		return nil, err
	}

	for _, post := range posts {
		post.Uri = utils.HydrateUri(post.DehydratedUri, "app.bsky.feed.post")

	}

	return posts, nil
}

func (d *DBxTablePosts) SelectPostIdByEpoch(epoch int64) (int64, error) {
	row := d.QueryRow("SELECT post_id FROM posts WHERE created_at <= $1 ORDER BY created_at DESC, post_id DESC LIMIT 1", epoch)

	if row == nil {
		return 0, nil
	}

	postid := int64(0)
	err := row.Scan(&postid)
	if (err != nil) && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}

	return postid, nil
}

func (d *DBxTablePosts) SelectPostIdByEpochAndRkey(epoch int64, rkey string) (int64, error) {
	row := d.QueryRow("SELECT post_id FROM posts WHERE created_at = $1 AND uri LIKE $2 ORDER BY created_at DESC, post_id DESC LIMIT 1", epoch, fmt.Sprintf("%%/%s", rkey))

	if row == nil {
		return 0, nil
	}

	postid := int64(0)
	err := row.Scan(&postid)
	if (err != nil) && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}

	return postid, nil
}

func (d *DBxTablePosts) SelectUnlabeled(cutoff int64, limit int) ([]*PostRow, error) {
	postrows := make([]*PostRow, 0, limit)
	err := d.Select(&postrows, "SELECT * FROM posts WHERE created_at < $1 AND labeled = 0 ORDER BY post_id ASC LIMIT $2", cutoff, limit)
	if err != nil {
		return nil, err
	}

	for _, post := range postrows {
		post.Uri = utils.HydrateUri(post.DehydratedUri, "app.bsky.feed.post")

	}

	return postrows, nil
}

func (d *DBxTablePosts) MarkLabeled(postid int64) error {
	_, err := d.Exec("UPDATE posts SET labeled = 1 WHERE post_id = ?", postid)
	if err != nil {
		return err
	}

	return nil
}

func (d *DBxTablePosts) InsertPost(p *PostRow) (*PostRow, error) {
	stmt, err := d.findOrPrepareNamedStmt("INSERT INTO posts (uri, actor_id, created_at, labeled) VALUES (:uri, :actor_id, :created_at, :labeled)")
	if err != nil {
		return nil, err
	}

	res, err := stmt.Exec(p)
	if err != nil {
		return nil, err
	}

	if p.PostId == 0 {
		p.PostId, err = res.LastInsertId()
		if err != nil {
			return nil, err
		}
	}

	return p, nil
}

func (d *DBxTablePosts) DeletePost(postid int64) error {
	stmt, err := d.findOrPrepareStmt("DELETE FROM posts WHERE post_id = $1")
	if err != nil {
		return err
	}
	_, err = stmt.Exec(postid)
	return err
}
