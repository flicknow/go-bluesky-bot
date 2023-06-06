package dbx

import (
	"database/sql"
	"errors"
	"path/filepath"

	"github.com/jmoiron/sqlx"
)

type FollowIndexedRow struct {
	FollowIndexedId int64 `db:"follow_indexed_id"`
	ActorId         int64 `db:"actor_id"`
	Created         bool
	Cursor          string `db:"cursor"`
	LastFollow      int64  `db:"last_follow"`
}

type DBxTableFollowsIndexed struct {
	*sqlx.DB `dbx-table:"follows_indexed" dbx-pk:"follow_indexed_id"`
	path     string
}

var FollowIndexedSchema = `
CREATE TABLE IF NOT EXISTS follows_indexed (
	follow_indexed_id INTEGER PRIMARY KEY,
	actor_id INTEGER NOT NULL UNIQUE,
	cursor TEXT DEFAULT "",
	last_follow INTEGER DEFAULT -1
);
CREATE INDEX IF NOT EXISTS idx_follow_indexed_last_follow
ON follows_indexed(last_follow);
`

func NewFollowsIndexedTable(dir string) *DBxTableFollowsIndexed {
	path := filepath.Join(dir, "follows-indexed.db")
	return &DBxTableFollowsIndexed{
		SQLxMustOpen(path, FollowIndexedSchema),
		path,
	}
}

func (d *DBxTableFollowsIndexed) FindByActorId(actorid int64) (*FollowIndexedRow, error) {
	row := d.QueryRowx("SELECT * FROM follows_indexed WHERE actor_id = ?", actorid)
	if row == nil {
		return nil, nil
	}

	indexed := &FollowIndexedRow{}
	err := row.StructScan(indexed)
	if (err != nil) && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	} else if err != nil {
		return nil, nil
	}

	return indexed, nil
}

func (d *DBxTableFollowsIndexed) FindOrCreateByActorId(actorid int64) (*FollowIndexedRow, error) {
	row, err := d.FindByActorId(actorid)
	if err != nil {
		return nil, err
	} else if row != nil {
		return row, nil
	}

	indexed := &FollowIndexedRow{ActorId: actorid, LastFollow: -1}
	res, err := d.NamedExec("INSERT OR IGNORE INTO follows_indexed (actor_id, last_follow) VALUES (:actor_id, :last_follow)", indexed)
	if err != nil {
		return nil, err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	} else if affected > 0 {
		indexedid, err := res.LastInsertId()
		if err != nil {
			return nil, err
		}

		indexed.Created = true
		indexed.FollowIndexedId = indexedid
		return indexed, nil
	}

	return d.FindByActorId(actorid)
}

func (d *DBxTableFollowsIndexed) SetLastFollow(actorid int64, followid int64) error {
	_, err := d.Exec("UPDATE follows_indexed SET last_follow = ? WHERE actor_id = ?", followid, actorid)
	return err
}

func (d *DBxTableFollowsIndexed) SetCursor(actorid int64, cursor string) error {
	_, err := d.Exec("UPDATE follows_indexed SET cursor = ? WHERE actor_id = ?", cursor, actorid)
	return err
}

func (d *DBxTableFollowsIndexed) SelectUnindexed(limit int) ([]*FollowIndexedRow, error) {
	indexes := make([]*FollowIndexedRow, 0, limit)
	err := d.Select(&indexes, "SELECT * FROM follows_indexed WHERE last_follow == -1 ORDER BY follow_indexed_id ASC LIMIT $1", limit)
	if err != nil {
		return nil, err
	}
	return indexes, nil
}
