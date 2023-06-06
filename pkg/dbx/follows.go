package dbx

import (
	"database/sql"
	"errors"
	"path/filepath"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/jmoiron/sqlx"
)

type followCacheEntry struct {
	follows []int64
	last    int64
}
type FollowRow struct {
	FollowId  int64  `db:"follow_id"`
	Rkey      string `db:"rkey"`
	ActorId   int64  `db:"actor_id"`
	SubjectId int64  `db:"subject_id"`
	CreatedAt int64  `db:"created_at"`
}

type DBxTableFollows struct {
	*sqlx.DB `dbx-table:"follows" dbx-pk:"follow_id"`
	Path     string
	cache    *lru.Cache[int64, *followCacheEntry]
}

var FollowSchema = `
CREATE TABLE IF NOT EXISTS follows (
	follow_id INTEGER PRIMARY KEY,
	rkey TEXT NOT NULL,
	actor_id INTEGER DEFAULT 0,
	subject_id INTEGER NOT NULL,
	created_at INTEGER NOT NULL,
	UNIQUE(actor_id, rkey) ON CONFLICT IGNORE,
	UNIQUE(subject_id, actor_id) ON CONFLICT IGNORE
);
CREATE INDEX IF NOT EXISTS idx_follows_actor_id
ON follows(actor_id);
CREATE INDEX IF NOT EXISTS idx_follows_actor_subject_id
ON follows(actor_id, subject_id);
`

func NewFollowsTable(dir string, cacheSize int) *DBxTableFollows {
	path := filepath.Join(dir, "follows.db")

	cache, err := lru.New[int64, *followCacheEntry](cacheSize)
	if err != nil {
		panic((err))
	}

	return &DBxTableFollows{
		SQLxMustOpen(path, FollowSchema),
		path,
		cache,
	}
}

func (d *DBxTableFollows) FindByRkey(actorid int64, rkey string) (*FollowRow, error) {
	row := d.QueryRowx("SELECT * FROM follows WHERE actor_id = $1 AND rkey = $2", actorid, rkey)
	if row == nil {
		return nil, nil
	}

	follow := &FollowRow{}
	err := row.StructScan(follow)
	if (err != nil) && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	} else if err != nil {
		return nil, nil
	}

	return follow, nil
}

func (d *DBxTableFollows) InsertFollow(rows ...*FollowRow) (*FollowRow, error) {
	if (rows == nil) || (len(rows) == 0) {
		return nil, nil
	}

	tx, err := d.Beginx()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var res sql.Result = nil
	for _, row := range rows {
		res, err = tx.NamedExec("INSERT OR IGNORE INTO follows (rkey, actor_id, subject_id, created_at) VALUES (:rkey, :actor_id, :subject_id, :created_at)", row)
		if err != nil {
			return nil, err
		}
	}

	lastid, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}

	last := rows[len(rows)-1]
	last.FollowId = lastid

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return last, nil
}

func (d *DBxTableFollows) FindLastFollow(actorid int64) (*FollowRow, error) {
	row := d.QueryRowx("SELECT * FROM follows WHERE actor_id = ? ORDER BY follow_id DESC", actorid)
	if row == nil {
		return nil, nil
	}

	follow := &FollowRow{}
	err := row.StructScan(follow)
	if (err != nil) && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	} else if err != nil {
		return nil, nil
	}

	return follow, nil
}

func (d *DBxTableFollows) SelectFollows(actorid int64, after int64, limit int) ([]*FollowRow, error) {
	follows := make([]*FollowRow, 0, limit)
	err := d.Select(&follows, "SELECT * FROM follows WHERE actor_id = $1 AND follow_id > $2 ORDER BY follow_id ASC LIMIT $3", actorid, after, limit)
	if err != nil {
		return nil, err
	}

	return follows, err
}
