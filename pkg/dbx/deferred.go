package dbx

type DeferredInt64 struct {
	ch   chan bool
	done bool
	val  int64
}

func NewDeferredInt64() *DeferredInt64 {
	return &DeferredInt64{
		ch: make(chan bool, 1),
	}
}

func (d *DeferredInt64) Done(val int64) {
	d.val = val
	d.done = true
	d.ch <- true
	close(d.ch)
}

func (d *DeferredInt64) Cancel() {
	if d.done {
		return
	}
	d.done = true
	close(d.ch)
}

func (d *DeferredInt64) Get() int64 {
	<-d.ch
	return d.val
}

type DeferredInt64s struct {
	ch   chan bool
	done bool
	val  []int64
}

func NewDeferredInt64s() *DeferredInt64s {
	return &DeferredInt64s{
		ch: make(chan bool, 1),
	}
}

func (d *DeferredInt64s) Done(val []int64) {
	d.val = val
	d.done = true
	d.ch <- true
	close(d.ch)
}

func (d *DeferredInt64s) Cancel() {
	if d.done {
		return
	}
	d.done = true
	close(d.ch)
}

func (d *DeferredInt64s) Get() []int64 {
	<-d.ch
	return d.val
}

type DeferredPost struct {
	ch   chan bool
	done bool
	val  *PostRow
}

func NewDeferredPost() *DeferredPost {
	return &DeferredPost{
		ch: make(chan bool, 1),
	}
}

func (d *DeferredPost) Done(val *PostRow) {
	d.val = val
	d.done = true
	d.ch <- true
	close(d.ch)
}

func (d *DeferredPost) Get() *PostRow {
	<-d.ch
	return d.val
}

func (d *DeferredPost) Cancel() {
	if d.done {
		return
	}
	d.done = true
	close(d.ch)
}
