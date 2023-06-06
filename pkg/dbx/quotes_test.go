package dbx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDBxSelectQuotes(t *testing.T) {
	d, cleanup := NewTestDBx()
	defer cleanup()

	actor := d.CreateActor()

	quotee := d.CreateActor()
	quoted := d.CreatePost(&TestPostRefInput{Actor: quotee.Did})
	quote := d.CreatePost(&TestPostRefInput{Actor: actor.Did, Quote: quoted.Uri})
	d.CreatePost(&TestPostRefInput{Actor: actor.Did, Reply: quoted.Uri})

	quoteeMentions, err := d.SelectQuotes(SQLiteMaxInt, 10, quotee.Did)
	if err != nil {
		panic(err)
	}
	assert.Equal(
		t,
		[]*PostRow{quote},
		quoteeMentions,
	)

}
