package utils

import (
	"fmt"
	"math/rand"
)

func NewTestDid() string {
	return fmt.Sprintf("did:plc:%d", rand.Int63())
}
