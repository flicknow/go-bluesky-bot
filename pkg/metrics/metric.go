package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/flicknow/go-bluesky-bot/pkg/utils"
)

type Metric interface {
	Name() string
	Value() int64
}

type Collector struct {
	enabled       bool
	mu            *sync.Mutex
	Events        []Metric
	DeleteFollows []Metric
	DeletePosts   []Metric
	InsertFollows []Metric
	InsertPosts   []Metric
}

func NewCollector(enabled bool) *Collector {
	return &Collector{
		enabled:       enabled,
		mu:            &sync.Mutex{},
		Events:        []Metric{},
		DeleteFollows: []Metric{},
		DeletePosts:   []Metric{},
		InsertFollows: []Metric{},
		InsertPosts:   []Metric{},
	}
}

func (c *Collector) DeleteFollow(val *DeleteFollow) {
	if !c.enabled {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.Events = append(c.Events, val)
	c.DeleteFollows = append(c.DeleteFollows, val)
}
func (c *Collector) DeletePost(val *DeletePost) {
	if !c.enabled {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.Events = append(c.Events, val)
	c.DeletePosts = append(c.DeletePosts, val)
}

func (c *Collector) InsertFollow(val *InsertFollow) {
	if !c.enabled {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.Events = append(c.Events, val)
	c.InsertFollows = append(c.InsertFollows, val)
}
func (c *Collector) InsertPost(val *InsertPost) {
	if !c.enabled {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.Events = append(c.Events, val)
	c.InsertPosts = append(c.InsertPosts, val)
}

func (c *Collector) Reset() *Collector {
	if !c.enabled {
		return c
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	copied := &Collector{
		enabled:       c.enabled,
		Events:        make([]Metric, len(c.Events)),
		DeleteFollows: make([]Metric, len(c.DeleteFollows)),
		DeletePosts:   make([]Metric, len(c.DeletePosts)),
		InsertFollows: make([]Metric, len(c.InsertFollows)),
		InsertPosts:   make([]Metric, len(c.InsertPosts)),
	}

	copy(copied.Events, c.Events)
	copy(copied.DeleteFollows, c.DeleteFollows)
	copy(copied.DeletePosts, c.DeletePosts)
	copy(copied.InsertFollows, c.InsertFollows)
	copy(copied.InsertPosts, c.InsertPosts)

	c.Events = make([]Metric, 0, len(c.Events))
	c.DeleteFollows = make([]Metric, 0, len(c.DeleteFollows))
	c.DeletePosts = make([]Metric, 0, len(c.DeletePosts))
	c.InsertFollows = make([]Metric, 0, len(c.InsertFollows))
	c.InsertPosts = make([]Metric, 0, len(c.InsertPosts))

	return copied
}

func (c *Collector) Report() string {
	if !c.enabled {
		return ""
	}

	prev := c.Reset()

	results := make([]string, 5)

	results[0] = fmt.Sprintf("All(%d)| %s\n", len(prev.Events), reportPercentiles(percentiles(prev.Events)))
	results[1] = fmt.Sprintf("DeleteFollows(%d)| %s\n", len(prev.DeleteFollows), reportPercentiles(percentiles(prev.DeleteFollows)))
	results[2] = fmt.Sprintf("DeletePosts(%d)| %s\n", len(prev.DeletePosts), reportPercentiles(percentiles(prev.DeletePosts)))
	results[3] = fmt.Sprintf("InsertFollows(%d)| %s\n", len(prev.InsertFollows), reportPercentiles(percentiles(prev.InsertFollows)))
	results[4] = fmt.Sprintf("InsertPosts(%d)| %s\n", len(prev.InsertPosts), reportPercentiles(percentiles(prev.InsertPosts)))

	return strings.Join(results, "\n")
}

type Percentile struct {
	Percentage int
	Value      Metric
}

func percentiles(vals []Metric) []*Percentile {
	vals = sortMetrics(vals)

	n := len(vals)
	if n == 0 {
		return []*Percentile{}
	}

	percentiles := make([]*Percentile, 0, 5)
	for _, percentage := range []int{50, 75, 90, 95, 99} {
		i := int(0.01 * float64(percentage) * float64(n))
		percentiles = append(percentiles, &Percentile{percentage, vals[i]})
	}

	return percentiles
}

func reportPercentiles(percentiles []*Percentile) string {
	return utils.Dump(percentiles)

	lines := make([]string, 0, len(percentiles))
	for _, percentile := range percentiles {
		lines = append(lines, fmt.Sprintf("%d: %+v", percentile.Percentage, percentile.Value))
	}

	return strings.Join(lines, ", ")
}

func sortMetrics(unsorted []Metric) []Metric {
	sorted := make([]Metric, len(unsorted))
	for i, val := range unsorted {
		sorted[i] = val
	}

	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Value() < sorted[j].Value() })

	return sorted
}
