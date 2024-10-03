package dbx

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/jmoiron/sqlx"
)

type ActorRow struct {
	ActorId   int64  `db:"actor_id"`
	Birthday  int64  `db:"birthday"`
	Blocked   bool   `db:"blocked"`
	Did       string `db:"did"`
	Created   bool
	CreatedAt int64 `db:"created_at"`
	LastPost  int64 `db:"last_post"`
	Posts     int64 `db:"posts"`
}

type DBxTableActors struct {
	*sqlx.DB        `dbx-table:"actors" dbx-pk:"actor_id"`
	path            string
	cache           *lru.Cache[string, *ActorRow]
	NamedStatements map[string]*sqlx.NamedStmt
	Statements      map[string]*sqlx.Stmt
}

var ActorSchema = `
CREATE TABLE IF NOT EXISTS actors (
	actor_id INTEGER PRIMARY KEY,
	birthday INTEGER DEFAULT 0,
	did TEXT NOT NULL UNIQUE,
	blocked INTEGER DEFAULT 0,
	created_at INTEGER DEFAULT 0,
	last_post INTEGER DEFAULT 0,
	posts INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_actors_blocked_birthday
ON actors(blocked, birthday);
CREATE INDEX IF NOT EXISTS idx_actors_created_at
ON actors(created_at);
CREATE INDEX IF NOT EXISTS idx_actors_blocked_created_at
ON actors(blocked, created_at);
`

func NewActorTable(dir string, cacheSize int) *DBxTableActors {
	path := filepath.Join(dir, "actors.db")

	cache, err := lru.New[string, *ActorRow](cacheSize)
	if err != nil {
		panic((err))
	}

	return &DBxTableActors{
		SQLxMustOpen(path, ActorSchema),
		path,
		cache,
		make(map[string]*sqlx.NamedStmt),
		make(map[string]*sqlx.Stmt),
	}
}

func (d *DBxTableActors) findOrPrepareNamedStmt(q string) (*sqlx.NamedStmt, error) {
	stmt := d.NamedStatements[q]
	if stmt != nil {
		return stmt, nil
	}

	var err error
	stmt, err = d.PrepareNamed(q)
	if err != nil {
		return nil, err
	}

	d.NamedStatements[q] = stmt
	return stmt, err
}
func (d *DBxTableActors) findOrPrepareStmt(q string) (*sqlx.Stmt, error) {
	stmt := d.Statements[q]
	if stmt != nil {
		return stmt, nil
	}

	var err error
	stmt, err = d.Preparex(q)
	if err != nil {
		return nil, err
	}

	d.Statements[q] = stmt
	return stmt, err
}

func (d *DBxTableActors) FindActorById(actorid int64) (*ActorRow, error) {
	row := d.QueryRowx("SELECT * FROM actors WHERE actor_id = ?", actorid)
	if row == nil {
		return nil, fmt.Errorf("could not find actor with id %d", actorid)
	}

	actorRow := &ActorRow{ActorId: actorid}
	err := row.StructScan(actorRow)
	if (err != nil) && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	} else if err != nil {
		return nil, nil
	}

	return actorRow, nil
}

func (d *DBxTableActors) FindActorsById(actorids []int64) ([]*ActorRow, error) {
	rows := make([]*ActorRow, 0, len(actorids))

	params := make([]any, len(actorids))
	plcs := make([]string, len(actorids))
	for i, actorid := range actorids {
		params[i] = actorid
		plcs[i] = "?"
	}

	err := d.DB.Select(
		&rows,
		fmt.Sprintf("SELECT * FROM actors WHERE actor_id IN (%s)", strings.Join(plcs, ",")),
		params...,
	)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (d *DBxTableActors) findActor(did string) (*ActorRow, error) {
	stmt, err := d.findOrPrepareStmt("SELECT * FROM actors WHERE did = ?")
	if err != nil {
		return nil, err
	}

	row := stmt.QueryRowx(did)
	if row == nil {
		return nil, fmt.Errorf("could not find actor with did %s", did)
	}

	actorRow := &ActorRow{Did: did}
	err = row.StructScan(actorRow)
	if (err != nil) && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	} else if err != nil {
		return nil, nil
	}

	return actorRow, nil
}

func (d *DBxTableActors) FindActor(did string) (*ActorRow, error) {
	actorRow, ok := d.cache.Get(did)
	if ok && (actorRow != nil) {
		return actorRow, nil
	}

	actorRow, err := d.findActor(did)
	if err != nil {
		return nil, err
	}

	d.cache.Add(did, actorRow)
	return actorRow, nil
}

func (d *DBxTableActors) FindActors(dids []string) ([]*ActorRow, error) {
	cached := make([]*ActorRow, 0, len(dids))
	lookup := make([]string, 0, len(dids))

	for _, did := range dids {
		actorRow, ok := d.cache.Get(did)
		if ok && (actorRow != nil) {
			cached = append(cached, actorRow)
		} else {
			lookup = append(lookup, did)
		}
	}

	if len(cached) == len(dids) {
		return cached, nil
	}

	found := make([]*ActorRow, 0, len(lookup))
	plcs := make([]string, len(lookup))
	params := make([]any, len(lookup))
	for i, did := range lookup {
		plcs[i] = "?"
		params[i] = did
	}

	q := fmt.Sprintf("SELECT * FROM actors WHERE did IN (%s)", strings.Join(plcs, ","))
	stmt, err := d.findOrPrepareStmt(q)
	if err != nil {
		return nil, err
	}

	err = stmt.Select(&found, params...)
	if err != nil {
		return nil, err
	}

	for _, actor := range found {
		d.cache.Add(actor.Did, actor)
		cached = append(cached, actor)
	}

	return cached, nil
}

func (d *DBxTableActors) FindOrCreateActors(dids []string) ([]*ActorRow, error) {
	if len(dids) == 1 {
		row, err := d.FindOrCreateActor(dids[0])
		if err != nil {
			return nil, err
		} else {
			return []*ActorRow{row}, nil
		}
	}

	found, err := d.FindActors(dids)
	if err != nil {
		return nil, err
	}
	if len(found) == len(dids) {
		return found, nil
	}

	exists := make(map[string]bool)
	for _, actor := range found {
		exists[actor.Did] = true
	}

	for _, did := range dids {
		if exists[did] {
			continue
		}
		actor, err := d.createActor(did)
		if err != nil {
			return nil, err
		}

		found = append(found, actor)
	}

	return found, nil
}

func (d *DBxTableActors) createActor(did string) (*ActorRow, error) {
	stmt, err := d.findOrPrepareStmt("INSERT OR IGNORE INTO actors (did) VALUES (?)")
	if err != nil {
		return nil, err
	}

	res, err := stmt.Exec(did)
	if err != nil {
		return nil, err
	}

	actorRow := &ActorRow{Did: did}
	affected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	} else if affected > 0 {
		actorId, err := res.LastInsertId()
		if err != nil {
			return nil, err
		}

		actorRow.Created = true
		actorRow.ActorId = actorId
		return actorRow, nil
	}

	return d.FindActor(did)
}

func (d *DBxTableActors) FindOrCreateActor(did string) (*ActorRow, error) {
	if did == "" {
		return nil, fmt.Errorf("did cannot be an empty string")
	}

	row, err := d.FindActor(did)
	if err != nil {
		return nil, err
	} else if row != nil {
		return row, nil
	}

	return d.createActor(did)
}

func (d *DBxTableActors) SelectActorsWithirthdaysBetween(start int64, end int64) ([]*ActorRow, error) {
	actors := make([]*ActorRow, 0)
	err := d.Select(&actors, "SELECT * FROM actors WHERE birthday > $1 AND birthday < $2 AND blocked == 0 ORDER BY created_at ASC", start, end)
	if err != nil {
		return nil, err
	}

	return actors, nil
}

func (d *DBxTableActors) InitializeBirthday(did string, birthday int64) (*ActorRow, error) {
	actor, err := d.findActor(did)
	if err != nil {
		return nil, err
	}

	_, err = d.DB.Exec("UPDATE actors SET birthday = ? WHERE actor_id = ?", birthday, actor.ActorId)
	if err != nil {
		return nil, err
	}
	actor.Birthday = birthday

	return actor, nil
}

func (d *DBxTableActors) SelectActorsWithoutBirthdays(cutoff int64, limit int) ([]*ActorRow, error) {
	actors := make([]*ActorRow, 0, limit)
	err := d.Select(&actors, "SELECT * FROM actors WHERE birthday == 0 AND blocked == 0 AND actor_id > $1 ORDER BY actor_id ASC LIMIT $2", cutoff, limit)
	if err != nil {
		return nil, err
	}

	return actors, nil
}

func (d *DBxTableActors) SelectUninitializedActors(cutoff int64, limit int) ([]*ActorRow, error) {
	actors := make([]*ActorRow, 0, limit)
	err := d.Select(&actors, "SELECT * FROM actors WHERE created_at == 0 AND blocked == 0 AND actor_id > $1 ORDER BY actor_id ASC LIMIT $2", cutoff, limit)
	if err != nil {
		return nil, err
	}

	return actors, nil
}

func (d *DBxTableActors) IncrementPostsCount(actorRow *ActorRow, now int64) error {
	stmt, err := d.findOrPrepareStmt("UPDATE actors SET last_post = ?, posts = posts + 1 WHERE actor_id = ?")
	if err != nil {
		return err
	}

	res, err := stmt.Exec(now, actorRow.ActorId)
	if err != nil {
		return err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		log.Printf("ERROR getting rows affected by updating post count for actor %d: %+v\n", actorRow.ActorId, err)
		return nil
	} else if affected == 0 {
		log.Printf("ERROR could not find actor %d\n", actorRow.ActorId)
		return nil
	}

	actorRow.LastPost = now
	actorRow.Posts = actorRow.Posts + 1
	d.cache.Add(actorRow.Did, actorRow)

	return nil
}

func (d *DBxTableActors) DecrementPostsCount(actorid int64, did string) error {
	stmt, err := d.findOrPrepareStmt("UPDATE actors SET posts = posts - 1 WHERE actor_id = ?")
	if err != nil {
		return err
	}

	res, err := stmt.Exec(actorid)
	if err != nil {
		return err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		log.Printf("ERROR getting rows affected by updating post count for actor %d: %+v\n", actorid, err)
		return nil
	} else if affected == 0 {
		log.Printf("ERROR could not find actor %d\n", actorid)
		return nil
	}

	if did == "" {
		return nil
	}

	actorRow, ok := d.cache.Get(did)
	if ok && (actorRow != nil) && (actorRow.Posts > 0) {
		actorRow.Posts = actorRow.Posts - 1
		d.cache.Add(did, actorRow)
	}

	return nil
}
