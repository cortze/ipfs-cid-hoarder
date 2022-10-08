package cid_source

import (
	"fmt"
	"reflect"
	"testing"
)

func TestRandomCidGen_GetNewCid(t *testing.T) {
	randomgen := NewRandomCidGen(6, 8)
	i := 0
	for true {
		fmt.Println(i)
		cid, err := randomgen.GetNewCid()
		if reflect.DeepEqual(cid, Undef) {
			break
		}
		if err != nil {
			t.Errorf("error %s while generating random cid", err)
		}
	}
	fmt.Println(randomgen.limit)
	if i > randomgen.limit {
		t.Errorf("i bigger than limit")
	}
}
