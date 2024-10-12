package blueskybot

import (
	"fmt"
	"math"
	"time"

	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/dbx"
	"github.com/labstack/gommon/log"
	cli "github.com/urfave/cli/v2"
)

func findMin(p *dbx.DBxTablePosts) int64 {
	var postid int64 = 0
	row := p.QueryRow("SELECT post_id FROM posts ORDER BY post_id ASC LIMIT 1")
	err := row.Scan(&postid)
	if err != nil {
		panic(err)
	}
	return postid
}

func findMax(p *dbx.DBxTablePosts) int64 {
	var postid int64 = 0
	row := p.QueryRow("SELECT post_id FROM posts ORDER BY post_id DESC LIMIT 1")
	err := row.Scan(&postid)
	if err != nil {
		panic(err)
	}
	return postid
}

func loadPartition(path string, src string, min int64, max int64) {
	p := dbx.SQLxMustOpen(path, `
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
	defer p.Close()

	p.MustExec(fmt.Sprintf("ATTACH DATABASE \"%s\" AS source", src))
	p.MustExec(`
		INSERT INTO posts SELECT * FROM source.posts
		WHERE post_id >= ? AND post_id < ? ORDER BY post_id ASC
	`, min, max)
	p.MustExec(`
		CREATE UNIQUE INDEX idx_unique_posts_uri
			ON posts(uri);
		CREATE INDEX idx_post_created_at
			ON posts(created_at DESC);
		CREATE INDEX idx_post_labeled
			ON posts(labeled, post_id DESC);
	`)
	p.MustExec(`ANALYZE`)

	p.MustExec("PRAGMA wal_checkpoint(TRUNCATE)")
}

var PerfCmd = &cli.Command{
	Name: "perf",
	Flags: cmd.CombineFlags(
		cmd.WithDb,
		&cli.StringFlag{
			Name:  "load",
			Usage: "file to partition from",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "sample",
			Usage: "file to load sample data from",
			Value: "",
		},
		&cli.Int64Flag{
			Name:  "sample-size",
			Usage: "number of sample rows to load",
			Value: 100000,
		},
	),
	Action: func(cctx *cli.Context) error {
		args := cctx.Args()
		count := args.Len()

		sample := cctx.String("sample")
		sampleSize := cctx.Int64("sample-size")

		load := cctx.String("load")
		if load != "" {
			src := dbx.NewPostTableWithPath(load)
			sampleMin := findMin(src)
			sampleMax := sampleMin + sampleSize
			loadPartition(sample, load, sampleMin, sampleMax)

			min := sampleMax
			max := findMax(src)
			rows := max - min

			partitionRows := int64(math.Floor((float64(rows) / float64(count))))
			for i, path := range args.Slice() {
				loadPartition(path, load, min+(int64(i)*partitionRows), min+(int64(i+1)*partitionRows))
			}

			return nil
		}

		if sample != "" {
			sampleDb := dbx.NewPostTableWithPath(sample)
			tables := make([]*dbx.DBxTablePosts, count)
			for i, path := range args.Slice() {
				tables[i] = dbx.NewPostTableWithPath(path)
			}

			rows, err := sampleDb.Queryx("SELECT * FROM posts ORDER BY post_id ASC")
			if err != nil {
				panic(err)
			}
			defer rows.Close()

			start := time.Now().UnixMicro()
			results := make([]chan int64, count)
			writers := make([]chan *dbx.PostRow, count)
			for i := range tables {
				result := make(chan int64, 1)
				results[i] = result

				writer := make(chan *dbx.PostRow, 1)
				writers[i] = writer
				table := tables[i]
				go func(table *dbx.DBxTablePosts, writer chan *dbx.PostRow, result chan int64) {
					stmt, err := table.PrepareNamed("INSERT INTO posts (uri, actor_id, created_at, labeled) VALUES (:uri, :actor_id, :created_at, :labeled)")
					if err != nil {
						panic(err)
					}
					for row := range writer {
						_, err = stmt.Exec(row)
						if err != nil {
							log.Panicf("db=%s row=%+v err=%+v\n", table.Path, row, err)
						}
					}
					result <- time.Now().UnixMicro() - start
					close(result)
				}(table, writer, result)
			}

			var count int64 = 0
			for rows.Next() {
				post := &dbx.PostRow{}
				err = rows.StructScan(post)
				if err != nil {
					panic(err)
				}

				for _, w := range writers {
					w <- post
				}

				count++
				if (sampleSize > 0) && (count > sampleSize) {
					break
				}
			}
			for _, w := range writers {
				close(w)
			}

			for _, result := range results {
				elapsed := <-result
				rate := float64(elapsed) / float64(count)
				fmt.Printf("> %.4f µs/row\n", rate)
			}
		}

		return nil
	},
}
