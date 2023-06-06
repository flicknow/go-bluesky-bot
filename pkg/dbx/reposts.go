package dbx

import (
	"fmt"
	"path/filepath"

	"github.com/jmoiron/sqlx"
	"github.com/flicknow/go-bluesky-bot/pkg/utils"
)

type RepostRow struct {
	RepostId      int64  `db:"repost_id"`
	ActorId       int64  `db:"actor_id"`
	DeHydratedUri string `db:"uri"`
	Uri           string
	SubjectId     int64 `db:"subject_id"`
	CreatedAt     int64 `db:"created_at"`
}

type DBxTableReposts struct {
	*sqlx.DB `dbx-table:"reposts" dbx-pk:"repost_id"`
	path     string
}

var RepostSchema = `
CREATE TABLE IF NOT EXISTS reposts (
	repost_id INTEGER PRIMARY KEY,
	uri TEXT NOT NULL UNIQUE,
	actor_id INTEGER DEFAULT 0,
	subject_id INTEGER NOT NULL,
	created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reposts_subject_id
ON reposts(subject_id);
`

func NewRepostsTable(dir string) *DBxTableReposts {
	path := filepath.Join(dir, "reposts.db")
	return &DBxTableReposts{
		SQLxMustOpen(path, RepostSchema),
		path,
	}
}

func (d *DBxTableReposts) FindByUri(uri string) (*RepostRow, error) {
	row := d.QueryRowx("SELECT * FROM reposts WHERE uri = ?", utils.DehydrateUri(uri))
	if row == nil {
		return nil, fmt.Errorf("could not find repost for uri %s", uri)
	}

	repost := &RepostRow{}
	err := row.StructScan(repost)
	if err != nil {
		return nil, err
	}

	repost.Uri = utils.HydrateUri(repost.DeHydratedUri, "app.bsky.feed.repost")
	return repost, nil
}
