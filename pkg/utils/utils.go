package utils

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"sort"
	"strings"
	"sync"

	lexutil "github.com/bluesky-social/indigo/lex/util"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func DehydrateUri(uri string) string {
	if len(uri) < 9 {
		return uri
	}

	indexes := indexN(uri[9:], "/", 3)
	if len(indexes) != 2 {
		return uri
	}

	return fmt.Sprintf("%s/%s", uri[9:(9+indexes[0])], uri[(10+indexes[1]):])
}

func HydrateUri(dehydrated string, lex string) string {
	indexes := indexN(dehydrated, "/", 2)
	if len(indexes) != 1 {
		return dehydrated
	}

	if (indexes[0] == 0) || (indexes[0] == (len(dehydrated) - 1)) {
		return dehydrated
	}

	return fmt.Sprintf("at://did:%s/%s/%s", dehydrated[:indexes[0]], lex, dehydrated[(indexes[0]+1):])
}

func indexN(s string, substr string, n int) []int {
	indexes := make([]int, 0, n)
	last := 0
	substrLen := len(substr)
	for i := 0; i < n; i++ {
		index := strings.Index(s[last:], substr)
		if index < 0 {
			break
		}
		pos := last + index
		indexes = append(indexes, pos)
		last = pos + substrLen
	}
	return indexes
}

func ParseRkey(uri string) string {
	if len(uri) < 5 {
		return ""
	}

	indexes := indexN(uri[5:], "/", 3)
	if len(indexes) != 2 {
		return ""
	}

	return uri[(5 + indexes[1] + 1):]
}

func DecodeCBOR(val cbg.CBORMarshaler, output any) error {
	decoder := lexutil.LexiconTypeDecoder{
		Val: val,
	}

	b, err := decoder.MarshalJSON()
	if err != nil {
		log.Printf("marshall error: %+v\n", val)
		return err
	}

	err = json.Unmarshal(b, &output)
	if err != nil {
		log.Printf("unmarshall error: %s\n", string(b))
		return err
	}

	return nil
}

func Dump(o any) string {
	bytes, err := json.Marshal(o)
	if err != nil {
		return ""
	}
	return string(bytes)
}

func ParseDid(uri string) string {
	if len(uri) <= 5 {
		log.Printf("ERROR could not parse did from %s: too short\n", uri)
		return ""
	}

	parts := strings.SplitN(uri[5:], "/", 3)
	if parts == nil {
		log.Printf("ERROR could not parse did from %s: no /\n", uri)
		return ""
	} else if len(parts) != 3 {
		log.Printf("ERROR could not parse did from %s: expected 2 / found %d\n", uri, len(parts))
		return ""
	}

	return parts[0]
}

func WriteFile(filename string, data []byte) error {
	dir := path.Dir(filename)
	base := path.Base(filename)

	tempFile, err := os.CreateTemp(dir, fmt.Sprintf(".%s.*", base))
	if err != nil {
		return err
	}
	defer os.Remove(tempFile.Name())

	_, err = tempFile.Write(data)
	if err != nil {
		return err
	}

	err = tempFile.Close()
	if err != nil {
		return nil
	}

	return os.Rename(tempFile.Name(), filename)
}

func SortInt64s(unsorted []int64) []int64 {
	sorted := make([]int64, len(unsorted))
	for i, val := range unsorted {
		sorted[i] = val
	}

	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	return sorted
}

func ParallelizeFuncs(funcs ...func() error) []error {
	var results = make([]error, len(funcs))

	var wg sync.WaitGroup
	for i, f := range funcs {
		wg.Add(1)
		go func(g func() error) {
			defer wg.Done()
			results[i] = g()
		}(f)
	}
	wg.Wait()

	var errs = make([]error, 0)
	for _, err := range errs {
		if err != nil {
			if strings.Contains(err.Error(), "could not find name") {
				panic(err)
			}
			errs = append(errs, err)
		}
	}

	return errs
}
