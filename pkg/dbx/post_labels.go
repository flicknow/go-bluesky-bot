package dbx

import (
	"path/filepath"

	"github.com/jmoiron/sqlx"
)

type PostLabelRow struct {
	PostLabelId int64 `db:"post_label_id"`
	PostId      int64 `db:"post_id"`
	LabelId     int64 `db:"label_id"`
}

type DBxTablePostLabels struct {
	*sqlx.DB `dbx-table:"post_labels" dbx-pk:"post_label_id"`
	path     string
}

var PostLabelSchema = `
CREATE TABLE IF NOT EXISTS post_labels (
	post_label_id INTEGER PRIMARY KEY,
	post_id INTEGER,
	label_id INTEGER,
	UNIQUE(post_id, label_id) ON CONFLICT IGNORE
);
CREATE INDEX IF NOT EXISTS idx_label_post_id
ON post_labels(label_id, post_id DESC);
`

func NewPostLabelTable(dir string) *DBxTablePostLabels {
	path := filepath.Join(dir, "post-labels.db")
	return &DBxTablePostLabels{
		SQLxMustOpen(path, PostLabelSchema),
		path,
	}
}

func (d *DBxTablePostLabels) InsertPostLabel(postid int64, labelids []int64) error {
	for _, labelid := range labelids {
		postlabel := &PostLabelRow{
			PostId:  postid,
			LabelId: labelid,
		}
		_, err := d.NamedExec("INSERT INTO post_labels (post_id, label_id) VALUES (:post_id, :label_id)", postlabel)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DBxTablePostLabels) SelectLabelsByPostId(postid int64) ([]int64, error) {
	var postlabels []int64 = make([]int64, 0)
	err := d.Select(&postlabels, "SELECT label_id FROM post_labels WHERE post_id = $1 ORDER BY label_id ASC", postid)
	if err != nil {
		return nil, err
	}
	return postlabels, nil
}

func (d *DBxTablePostLabels) SelectPostsByLabel(labelid int64, before int64, limit int) ([]int64, error) {
	q := `
SELECT
	post_id
FROM
	post_labels
WHERE
	label_id = $1
	AND post_id < $2
ORDER BY
	post_id DESC
LIMIT
	$3
`

	posts := make([]int64, 0, limit)
	rows, err := d.Queryx(q, labelid, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		post := &PostLabelRow{}
		err = rows.StructScan(&post)
		if err != nil {
			return nil, err
		}
		posts = append(posts, post.PostId)
	}

	return posts, nil
}

func (d *DBxTablePostLabels) DeletePostLabelsByPostId(postid int64) error {
	_, err := d.Exec("DELETE FROM post_labels WHERE post_id = $1", postid)
	return err
}
