package metrics

type InsertFollow struct {
	FindActors int64
	Insert     int64
	Index      struct {
		FindIndex     int64
		SetLastFollow int64
		Val           int64
	}
	Val int64
}

func NewInsertFollow() *InsertFollow {
	return &InsertFollow{
		FindActors: 0,
		Insert:     0,
		Index: struct {
			FindIndex     int64
			SetLastFollow int64
			Val           int64
		}{},
		Val: 0,
	}
}

func (d *InsertFollow) Value() int64 {
	return d.Val
}
func (d *InsertFollow) Name() string {
	return "InsertFollow"
}

type DeleteFollow struct {
	Val int64
}

func NewDeleteFollow() *DeleteFollow {
	return &DeleteFollow{}
}

func (d *DeleteFollow) Value() int64 {
	return d.Val
}
func (d *DeleteFollow) Name() string {
	return "DeleteFollow"
}
