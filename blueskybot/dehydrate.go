package blueskybot

import (
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"syscall"

	"github.com/jmoiron/sqlx"
	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/dbx"
	"github.com/flicknow/go-bluesky-bot/pkg/utils"
	cli "github.com/urfave/cli/v2"
)

type uriRow struct {
	Pk  int64
	Uri string
}

func SQLxOpenFast(path string, initsql string) *sqlx.DB {
	exists, err := dbx.DbExists(path)
	if err != nil {
		panic(fmt.Sprintf("error checking if sqlite db %s exists: %+v", path, err))
	}

	pool, err := sqlx.Open("sqlite3", fmt.Sprintf("file:%s?_busy_timeout=10000&_synchronous=0&_journal_mode=OFF&_locking_mode=EXCLUSIVE", path))
	if err != nil {
		panic(fmt.Sprintf("cannot open sqlite file %s: %+v", path, err))
	}

	if !exists {
		_, err := pool.Exec(initsql)
		if err != nil {
			panic(fmt.Sprintf("cannot initialize sqlite db %s: %+v", path, err))
		}
	}

	return pool
}

type LegacyFollowRow struct {
	FollowId      int64  `db:"follow_id"`
	DeHydratedUri string `db:"uri"`
	Uri           string
	ActorId       int64 `db:"actor_id"`
	SubjectId     int64 `db:"subject_id"`
	CreatedAt     int64 `db:"created_at"`
}

func deHydrateFollows(cctx *cli.Context) error {
	dbDir := cctx.String("db-dir")
	if dbDir == "" {
		dbDir = dbx.DefaultDbDir
	}

	dstFile, err := os.CreateTemp(dbDir, "")
	if err != nil {
		return err
	}
	defer func() {
		name := dstFile.Name()
		os.Remove(name)
		os.Remove(fmt.Sprintf("%s-shm", name))
		os.Remove(fmt.Sprintf("%s-wal", name))
	}()

	fmt.Printf("> creating tmp follows table: %s\n", dstFile.Name())
	dst := SQLxOpenFast(dstFile.Name(), `
		CREATE TABLE follows (
			follow_id INTEGER PRIMARY KEY,
			rkey TEXT NOT NULL,
			actor_id INTEGER DEFAULT 0,
			subject_id INTEGER NOT NULL,
			created_at INTEGER NOT NULL,
			UNIQUE(actor_id, rkey) ON CONFLICT IGNORE,
			UNIQUE(subject_id, actor_id) ON CONFLICT IGNORE
		);
	`)
	defer dst.Close()

	d := dbx.NewDBx(cmd.ToContext(cctx))
	src := d.Follows
	defer d.Close()

	tx, err := dst.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	chunk := 25
	last := int64(-1)
	seen := int64(0)
	for {
		rows := make([]*LegacyFollowRow, 0, chunk)
		err := src.Select(&rows, "SELECT * FROM follows WHERE follow_id > ? ORDER BY follow_id ASC LIMIT ?", last, chunk)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}

		for _, row := range rows {
			parts := strings.Split(row.DeHydratedUri, "/")
			if len(parts) == 0 {
				panic(fmt.Sprintf("cannot handle uri: %s", row.DeHydratedUri))
			}

			rkey := parts[len(parts)-1]
			follow := &dbx.FollowRow{
				FollowId:  row.FollowId,
				Rkey:      rkey,
				ActorId:   row.ActorId,
				SubjectId: row.SubjectId,
				CreatedAt: row.CreatedAt,
			}

			row.DeHydratedUri = utils.DehydrateUri(row.DeHydratedUri)
			_, err = tx.NamedExec("INSERT INTO follows (follow_id, rkey, actor_id, subject_id, created_at) VALUES (:follow_id, :rkey, :actor_id, :subject_id, :created_at)", follow)
			if err != nil {
				return err
			}
			seen++
		}

		last = rows[len(rows)-1].FollowId
		if (seen % 10000) == 0 {
			fmt.Printf("dehydrated %d follows\n", seen)
			err = tx.Commit()
			if err != nil {
				return err
			}
			tx, err = dst.Beginx()
			if err != nil {
				return err
			}
		}
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	dst.MustExec(`
		CREATE INDEX idx_follows_actor_subject_id
			ON follows(actor_id, subject_id);
		CREATE INDEX idx_follows_actor_id
			ON follows(actor_id);
	`)

	err = dst.Close()
	if err != nil {
		return err
	}

	tableVal := reflect.ValueOf(d.Follows).Elem()
	pathField := tableVal.FieldByName("path")
	path := pathField.String()

	fmt.Printf("renaming dehydrated follows table %s to %s\n", dstFile.Name(), path)
	return os.Rename(dstFile.Name(), path)
}

func deHydratePosts(cctx *cli.Context) error {
	dbDir := cctx.String("db-dir")
	if dbDir == "" {
		dbDir = dbx.DefaultDbDir
	}

	dstFile, err := os.CreateTemp(dbDir, "")
	if err != nil {
		return err
	}
	defer func() {
		name := dstFile.Name()
		os.Remove(name)
		os.Remove(fmt.Sprintf("%s-shm", name))
		os.Remove(fmt.Sprintf("%s-wal", name))
	}()

	dst := SQLxOpenFast(dstFile.Name(), `
		CREATE TABLE posts (
			post_id INTEGER PRIMARY KEY,
			uri TEXT NOT NULL,
			actor_id INTEGER DEFAULT 0,
			created_at INTEGER NOT NULL,
			labeled INTEGER DEFAULT 0,
			likes INTEGER DEFAULT 0,
			quotes INTEGER DEFAULT 0,
			replies INTEGER DEFAULT 0,
			reposts INTEGER DEFAULT 0
		);
	`)
	defer dst.Close()

	d := dbx.NewDBx(cmd.ToContext(cctx))
	src := d.Posts
	defer d.Close()

	tx, err := dst.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	chunk := 25
	last := int64(-1)
	seen := int64(0)
	for {
		rows := make([]*dbx.PostRow, 0, chunk)
		err := src.Select(&rows, "SELECT * FROM posts WHERE post_id > ? ORDER BY post_id ASC LIMIT ?", last, chunk)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}

		for _, row := range rows {
			row.DehydratedUri = utils.DehydrateUri(row.DehydratedUri)
			_, err = tx.NamedExec("INSERT INTO posts (post_id, uri, actor_id, created_at, labeled) VALUES (:post_id, :uri, :actor_id, :created_at, :labeled)", row)
			if err != nil {
				return err
			}
			seen++
		}

		last = rows[len(rows)-1].PostId
		if (seen % 10000) == 0 {
			fmt.Printf("dehydrated %d posts\n", seen)

			err = tx.Commit()
			if err != nil {
				return err
			}
			tx, err = dst.Beginx()
			if err != nil {
				return err
			}
		}
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	dst.MustExec(`
		CREATE UNIQUE INDEX idx_unique_posts_uri
			ON posts(uri);
		CREATE INDEX idx_post_created_at
			ON posts(created_at DESC);
		CREATE INDEX idx_post_labeled
			ON posts(labeled, post_id DESC);
		CREATE INDEX idx_post_actor
			ON posts(actor_id, post_id DESC);
	`)

	err = dst.Close()
	if err != nil {
		return err
	}

	fmt.Printf("renaming dehydrated posts table %s to %s\n", dstFile.Name(), d.Posts.Path)
	return os.Rename(dstFile.Name(), d.Posts.Path)
}

var DehydrateCmd = &cli.Command{
	Name: "dehydrate",
	Flags: cmd.CombineFlags(
		cmd.WithDb,
	),
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() == 0 {
			return nil
		}

		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc,
			syscall.SIGHUP,
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGQUIT)
		go func() {
			panic(<-sigc)
		}()

		for _, table := range cctx.Args().Slice() {
			switch table {
			case "posts":
				err := deHydratePosts(cctx)
				if err != nil {
					return err
				}
			case "follows":
				err := deHydrateFollows(cctx)
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("%s is not a dehydratable table", table)
			}
		}

		return nil
	},
}
