package indexer

import (
	"fmt"
	"regexp"
	"strings"

	"golang.org/x/text/unicode/norm"
)

var CeuSemLimitesRegex = regexp.MustCompile(norm.NFC.String(fmt.Sprintf(`(?is).*(%s).*`, strings.Join([]string{
	`(\bspaces\s+d[ao]\s+minaj\b)`,
	`(#(ceu|céu)semlimites\b)`,
	`(\b(ceu|céu)\s+sem\s+limites\s+do\s+@minaj.com.br\b)`,
}, "|"))))

func IsCeuSemLimites(text string) bool {
	return CeuSemLimitesRegex.MatchString(norm.NFC.String(text))
}
