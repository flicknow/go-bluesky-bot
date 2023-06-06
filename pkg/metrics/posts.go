package metrics

type InsertPost struct {
	Insert      int64
	InsertReply struct {
		Insert int64
		Val    int64
	}
	FindPosts                    int64
	FindMentionedAndQuotedActors int64
	InsertQuote                  struct {
		Insert int64
		Val    int64
	}
	InsertMentions struct {
		Insert int64
		Val    int64
	}
	InsertLabels struct {
		FindLabels int64
		Insert     int64
		Val        int64
	}
	FindMentionedActors int64
	InsertDMs           struct {
		FindDMs int64
		Insert  int64
		Val     int64
	}
	InsertThreadMentions struct {
		FindThreadMentions int64
		Insert             int64
		Val                int64
	}
	UpdateActor int64
	Val         int64
}

func NewInsertPost() *InsertPost {
	return &InsertPost{
		Insert: 0,
		InsertReply: struct {
			Insert int64
			Val    int64
		}{},
		FindPosts:                    0,
		FindMentionedAndQuotedActors: 0,
		InsertQuote: struct {
			Insert int64
			Val    int64
		}{},
		InsertMentions: struct {
			Insert int64
			Val    int64
		}{},
		InsertLabels: struct {
			FindLabels int64
			Insert     int64
			Val        int64
		}{},
		FindMentionedActors: 0,
		InsertDMs: struct {
			FindDMs int64
			Insert  int64
			Val     int64
		}{},
		InsertThreadMentions: struct {
			FindThreadMentions int64
			Insert             int64
			Val                int64
		}{},
		UpdateActor: 0,
		Val:         0,
	}
}

func (d *InsertPost) Name() string {
	return "InsertPost"
}

func (d *InsertPost) Value() int64 {
	return d.Val
}

type DeletePost struct {
	FindPost       int64
	DeleteMentions struct {
		FindMentions int64
		Delete       int64
		Val          int64
	}
	DeleteThreadMentions struct {
		FindThreadMentions int64
		Delete             int64
		Val                int64
	}
	DeleteDMs struct {
		FindDMs int64
		Delete  int64
		Val     int64
	}
	DeleteLabels struct {
		FindLabels int64
		Delete     int64
		Val        int64
	}
	DeleteReply struct {
		FindReply int64
		Delete    int64
		Val       int64
	}
	DeleteQuote struct {
		FindQuote int64
		Delete    int64
		Val       int64
	}
	Delete      int64
	UpdateActor int64
	Val         int64
}

func (d *DeletePost) Name() string {
	return "DeletePost"
}

func (d *DeletePost) Value() int64 {
	return d.Val
}
func NewDeletePost() *DeletePost {
	return &DeletePost{
		FindPost: 0,
		DeleteMentions: struct {
			FindMentions int64
			Delete       int64
			Val          int64
		}{},
		DeleteThreadMentions: struct {
			FindThreadMentions int64
			Delete             int64
			Val                int64
		}{},
		DeleteDMs: struct {
			FindDMs int64
			Delete  int64
			Val     int64
		}{},
		DeleteLabels: struct {
			FindLabels int64
			Delete     int64
			Val        int64
		}{},
		DeleteReply: struct {
			FindReply int64
			Delete    int64
			Val       int64
		}{},
		DeleteQuote: struct {
			FindQuote int64
			Delete    int64
			Val       int64
		}{},
		Delete:      0,
		UpdateActor: 0,
		Val:         0,
	}
}
