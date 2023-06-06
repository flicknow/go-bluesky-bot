package indexer

import (
	"regexp"
	"strings"

	"github.com/bluesky-social/indigo/api/bsky"
)

var GeneralMotorsRegex = regexp.MustCompile(`.*\bGM\b.*`)
var GoodMorningRegex = regexp.MustCompile(`(?i)((\b((g'?’?m+(orning?)?)|(g[ou]+d\s?morning?))\b)|(((\(gm\))|(\bgm btw\b))))`)
var GoodNightRegex = regexp.MustCompile(`(?i)\b((g'?’?n+(ight|ite)?)|(g[ou]+d\s?(night|nite)))\b`)
var GoodAfternoonRegex = regexp.MustCompile(`(?i)\b(g[ou]+d\s?(after)?noon)\b`)
var GoodEveningRegex = regexp.MustCompile(`(?i)\b(g[ou]+d\s?evening?)\b`)
var OnlyRegex = regexp.MustCompile(`(?i).*\bto\b.*\bonly\b`)

func isGoodMorningSkeet(post *bsky.FeedPost) bool {
	text := post.Text
	if !GoodMorningRegex.MatchString(text) {
		return false
	}

	// if they have a link, and it's not a bsky link
	// maybe it's a General Motors news article and we should skip it
	if GeneralMotorsRegex.MatchString(text) {
		for _, facet := range post.Facets {
			for _, feature := range facet.Features {
				link := feature.RichtextFacet_Link
				if link != nil {
					if !strings.Contains(link.Uri, "/bsky.app/") {
						return false
					}
				}
			}
		}
	}

	return true
}

func isGoodNightSkeet(text string) bool {
	return GoodNightRegex.MatchString(text)
}

func isGoodAfternoonSkeet(text string) bool {
	return GoodAfternoonRegex.MatchString(text)
}

func isGoodEveningSkeet(text string) bool {
	return GoodEveningRegex.MatchString(text)
}

func isGm(post *bsky.FeedPost) bool {
	text := post.Text
	if isGoodMorningSkeet(post) {
		return true
	} else if isGoodNightSkeet(text) {
		return true
	} else if isGoodAfternoonSkeet(text) {
		return true
	} else if isGoodEveningSkeet(text) {
		return true
	} else {
		return false
	}
}
