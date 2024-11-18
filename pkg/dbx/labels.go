package dbx

import (
	"database/sql"
	"errors"
	"path/filepath"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/jmoiron/sqlx"
)

type LabelRow struct {
	LabelId int64  `db:"label_id"`
	Name    string `db:"name"`
}

type DBxTableLabels struct {
	*sqlx.DB `dbx-table:"labels" dbx-pk:"label_id"`
	path     string
	cache    *lru.Cache[string, *LabelRow]
}

var LabelSchema = `
CREATE TABLE IF NOT EXISTS labels (
	label_id INTEGER PRIMARY KEY,
	name TEXT NOT NULL UNIQUE
);
`

func NewLabelTable(dir string, cacheSize int) *DBxTableLabels {
	path := filepath.Join(dir, "labels.db")

	cache, err := lru.New[string, *LabelRow](cacheSize)
	if err != nil {
		panic((err))
	}

	return &DBxTableLabels{
		SQLxMustOpen(path, LabelSchema),
		path,
		cache,
	}
}

func (d *DBxTableLabels) FindLabelByLabelId(labelid int64) (*LabelRow, error) {
	row := d.QueryRowx("SELECT * FROM labels WHERE label_id = ?", labelid)
	if row == nil {
		return nil, nil
	}

	labelRow := &LabelRow{LabelId: labelid}
	err := row.StructScan(labelRow)
	if (err != nil) && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	} else if err != nil {
		return nil, nil
	}

	return labelRow, nil
}

func (d *DBxTableLabels) FindLabel(name string) (*LabelRow, error) {
	labelRow, ok := d.cache.Get(name)
	if ok && (labelRow != nil) {
		return labelRow, nil
	}

	row := d.QueryRow("SELECT label_id FROM labels WHERE name = ?", name)
	if row == nil {
		return nil, nil
	}

	labelRow = &LabelRow{Name: name}
	err := row.Scan(&labelRow.LabelId)
	if (err != nil) && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	} else if err != nil {
		return nil, nil
	}

	d.cache.Add(name, labelRow)
	return labelRow, nil
}

func (d *DBxTableLabels) FindOrCreateLabel(name string) (*LabelRow, error) {
	row, err := d.FindLabel(name)
	if err != nil {
		return nil, err
	} else if row != nil {
		return row, nil
	}

	res, err := d.Exec("INSERT OR IGNORE INTO labels (name) VALUES (?)", name)
	if err != nil {
		return nil, err
	}

	labelRow := &LabelRow{Name: name}
	affected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	} else if affected > 0 {
		labelId, err := res.LastInsertId()
		if err != nil {
			return nil, err
		}

		labelRow.LabelId = labelId
		return labelRow, nil
	}

	return d.FindLabel(name)
}

func (d *DBxTableLabels) SelectNewskieLabels() ([]*LabelRow, error) {
	labels := make([]*LabelRow, 0)
	err := d.Select(
		&labels,
		`SELECT * FROM labels WHERE name LIKE "newskie-%" ORDER BY name ASC`,
	)
	if err != nil {
		return nil, err
	}

	return labels, nil
}
