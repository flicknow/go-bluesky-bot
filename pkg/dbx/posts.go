package dbx

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/flicknow/go-bluesky-bot/pkg/utils"
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
	*sqlx.DB `dbx-table:"posts" dbx-pk:"post_id"`
	Path     string
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
	return &DBxTablePosts{
		SQLxMustOpen(path, PostSchema),
		path,
	}
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
	err := d.Select(&postrows, q, params...)
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
	postrow := &PostRow{}
	err := d.Get(postrow, "SELECT * FROM posts WHERE uri = ?", utils.DehydrateUri(uri))
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
	var postid int64 = 0
	err := d.Get(&postid, "SELECT post_id FROM posts WHERE uri = ?", utils.DehydrateUri(uri))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		} else {
			return 0, err
		}
	}
	return postid, nil
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
	res, err := d.Exec("UPDATE posts SET labeled = 1 WHERE post_id = ?", postid)
	if err != nil {
		return err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected != int64(1) {
		return fmt.Errorf("did not mark post %d labeled", postid)
	}

	return nil
}

func (d *DBxTablePosts) InsertPost(p *PostRow) (*PostRow, error) {
	res, err := d.NamedExec("INSERT INTO posts (uri, actor_id, created_at, labeled) VALUES (:uri, :actor_id, :created_at, :labeled)", p)
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
	_, err := d.Exec("DELETE FROM posts WHERE post_id = $1", postid)
	return err
}
