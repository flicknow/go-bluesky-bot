package dbx

import (
	"path/filepath"

	"github.com/jmoiron/sqlx"
)

const AccountLabelType = 0
const PostLabelType = 1

type CustomLabel struct {
	CustomLabelId int64  `db:"custom_label_id"`
	SubjectType   int64  `db:"subject_type"`
	SubjectId     int64  `db:"subject_id"`
	CreatedAt     int64  `db:"created_at"`
	LabelId       int64  `db:"label_id"`
	Neg           int64  `db:"neg"`
	Cbor          []byte `db:"cbor"`
}

type DBxTableCustomLabels struct {
	*sqlx.DB `dbx-table:"custom_labels" dbx-pk:"custom_label_id"`
	path     string
}

var CustomLabelSchema = `
CREATE TABLE IF NOT EXISTS custom_labels (
	custom_label_id INTEGER PRIMARY KEY,
	subject_type INTEGER,
	subject_id INTEGER,
	created_at INTEGER,
	label_id INTEGER,
	neg INTEGER DEFAULT 0,
	cbor BLOB,
	UNIQUE(label_id, subject_type, subject_id, neg) ON CONFLICT IGNORE
);
CREATE INDEX IF NOT EXISTS idx_custom_label_created_at
ON custom_labels(created_at);
CREATE INDEX IF NOT EXISTS idx_custom_label_label_id_created_at
ON custom_labels(label_id, created_at);
CREATE INDEX IF NOT EXISTS idx_custom_label_label_id_neg
ON custom_labels(label_id, neg);
`

func (d *DBxTableCustomLabels) InsertLabels(rows []*CustomLabel) error {
	tx, err := d.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, row := range rows {
		_, err := tx.NamedExec(
			"INSERT OR IGNORE INTO custom_labels (label_id, created_at, neg, subject_type, subject_id, cbor) VALUES (:label_id, :created_at, :neg, :subject_type, :subject_id, :cbor)",
			row,
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (d *DBxTableCustomLabels) SelectLabels(since int64, limit int) ([]*CustomLabel, error) {
	labels := make([]*CustomLabel, 0)
	err := d.Select(
		&labels,
		"SELECT * FROM custom_labels WHERE custom_label_id > $1 ORDER BY custom_label_id ASC LIMIT $2",
		since,
		limit,
	)
	if err != nil {
		return nil, err
	}

	return labels, nil

}

func (d *DBxTableCustomLabels) SelectLabelsByLabelIdAndNeg(labelId int64, isNeg bool, since int64, limit int) ([]*CustomLabel, error) {
	neg := 0
	if isNeg {
		neg = 1
	}

	labels := make([]*CustomLabel, 0)
	err := d.Select(
		&labels,
		"SELECT * FROM custom_labels WHERE label_id = $1 AND neg = $2 AND custom_label_id > $3 ORDER BY custom_label_id ASC LIMIT $4",
		labelId,
		neg,
		since,
		limit,
	)
	if err != nil {
		return nil, err
	}

	return labels, nil
}

func (d *DBxTableCustomLabels) SelectLabelsByLabelId(labelId int64, since int64, limit int) ([]*CustomLabel, error) {
	labels := make([]*CustomLabel, 0)
	err := d.Select(
		&labels,
		"SELECT * FROM custom_labels WHERE label_id = $1 AND custom_label_id > $2 ORDER BY custom_label_id ASC LIMIT $3",
		labelId,
		since,
		limit,
	)
	if err != nil {
		return nil, err
	}

	return labels, nil
}

func (d *DBxTableCustomLabels) DeleteLabelByPostId(labelId int64, postId int64) error {
	_, err := d.DB.Exec(
		"DELETE FROM custom_labels WHERE label_id = $1 AND subject_id = $2 AND subject_type = $3",
		labelId,
		postId,
		PostLabelType,
	)
	return err
}

func NewCustomLabelTable(dir string) *DBxTableCustomLabels {
	path := filepath.Join(dir, "custom-labels.db")
	return &DBxTableCustomLabels{
		SQLxMustOpen(path, CustomLabelSchema),
		path,
	}
}
