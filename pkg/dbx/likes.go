package dbx

import (
	"fmt"
	"path/filepath"

	"github.com/jmoiron/sqlx"
	"github.com/flicknow/go-bluesky-bot/pkg/utils"
)

type LikeRow struct {
	LikeId        int64  `db:"like_id"`
	ActorId       int64  `db:"actor_id"`
	DehydratedUri string `db:"uri"`
	Uri           string
	SubjectId     int64 `db:"subject_id"`
	CreatedAt     int64 `db:"created_at"`
}

type DBxTableLikes struct {
	*sqlx.DB `dbx-table:"likes" dbx-pk:"like_id"`
	path     string
}

var LikeSchema = `
CREATE TABLE IF NOT EXISTS likes (
	like_id INTEGER PRIMARY KEY,
	uri TEXT NOT NULL UNIQUE,
	actor_id INTEGER DEFAULT 0,
	subject_id INTEGER NOT NULL,
	created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_subject_id
ON likes(subject_id);
`

func NewLikesTable(dir string) *DBxTableLikes {
	path := filepath.Join(dir, "likes.db")
	return &DBxTableLikes{
		SQLxMustOpen(path, LikeSchema),
		path,
	}
}

func (d *DBxTableLikes) FindByUri(uri string) (*LikeRow, error) {
	row := d.QueryRowx("SELECT * FROM likes WHERE uri = ?", utils.DehydrateUri(uri))
	if row == nil {
		return nil, fmt.Errorf("could not find like for uri %s", uri)
	}

	like := &LikeRow{}
	err := row.StructScan(like)
	if err != nil {
		return nil, err
	}

	like.Uri = utils.HydrateUri(like.DehydratedUri, "app.bsky.feed.like")
	return like, nil
}
