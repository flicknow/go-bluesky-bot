package dbx

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/jmoiron/sqlx"
)

type DmRow struct {
	DmId    int64 `db:"dm_id"`
	PostId  int64 `db:"post_id"`
	ActorId int64 `db:"actor_id"`
}

type DBxTableDms struct {
	*sqlx.DB `dbx-table:"dms" dbx-pk:"dm_id"`
	path     string
}

var DmSchema = `
CREATE TABLE IF NOT EXISTS dms (
	dm_id INTEGER PRIMARY KEY,
	post_id INTEGER NOT NULL,
	actor_id INTEGER NOT NULL,
	UNIQUE(actor_id, post_id DESC) ON CONFLICT IGNORE
);
CREATE INDEX IF NOT EXISTS idx_dm_post_actor_id
ON dms(post_id, actor_id ASC);
`

func NewDmTable(dir string) *DBxTableDms {
	path := filepath.Join(dir, "dms.db")
	return &DBxTableDms{
		SQLxMustOpen(path, DmSchema),
		path,
	}
}

func (d *DBxTableDms) SelectDms(postid int64) ([]int64, error) {
	var mentions []int64 = make([]int64, 0)
	err := d.Select(&mentions, "SELECT actor_id FROM dms WHERE post_id = $1 ORDER BY actor_id ASC", postid)
	if err != nil {
		return nil, err
	}
	return mentions, nil
}

func (d *DBxTableDms) SelectDmsByActorId(actorid int64, before int64, limit int) ([]int64, error) {
	q := `
SELECT
	post_id
FROM
	dms
WHERE
	actor_id = $1
	AND post_id < $2
ORDER BY
	post_id DESC
LIMIT
	$3
`

	dms := make([]int64, 0, limit)
	rows, err := d.Queryx(q, actorid, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		dm := &DmRow{}
		err = rows.StructScan(&dm)
		if err != nil {
			return nil, err
		}
		dms = append(dms, dm.PostId)
	}

	return dms, nil
}

func (d *DBxTableDms) InsertDms(postid int64, actorids []int64) error {
	if len(actorids) == 0 {
		return nil
	}

	values := make([]string, len(actorids))
	for i, actorid := range actorids {
		values[i] = fmt.Sprintf("(%d, %d)", postid, actorid)
	}

	q := fmt.Sprintf("INSERT INTO dms (post_id, actor_id) VALUES %s", strings.Join(values, ","))
	_, err := d.Exec(q)

	return err
}

func (d *DBxTableDms) DeleteDmsByPostId(postid int64) error {
	_, err := d.Exec("DELETE FROM dms WHERE post_id = $1", postid)
	return err
}
