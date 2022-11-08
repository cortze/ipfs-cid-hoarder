package cid_source

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func TestRandomCidGen_GetNewCid(t *testing.T) {
	randomgen := NewRandomCidGen(6, 8)
	i := 0
	for true {
		cid, err := randomgen.GetNewCid()
		if reflect.DeepEqual(cid, Undef) {
			break
		}
		if err != nil {
			t.Errorf("error %s while generating random cid", err)
		}
	}
	if i > randomgen.limit {
		t.Errorf("i bigger than limit")
	}
}

func TestRandomCidGen_GetNewCidWithChannel(t *testing.T) {
	GetNewCidReturnTypeChannel := make(chan *GetNewCidReturnType, 10)
	var genWG sync.WaitGroup
	go routineForReceivingFromChannel(GetNewCidReturnTypeChannel, &genWG)
	randomgen := NewRandomCidGen(6, 8)
	i := 0
	for true {
		cid, err := randomgen.GetNewCid()
		if reflect.DeepEqual(cid, Undef) {
			break
		}
		if err != nil {
			t.Errorf("error %s while generating random cid", err)
		}
		GetNewCidReturnTypeChannel <- &cid
	}
	close(GetNewCidReturnTypeChannel)
	genWG.Wait()
	if i > randomgen.limit {
		t.Errorf("i bigger than limit")
	}
}

func routineForReceivingFromChannel(GetNewCidReturnTypeChannel <-chan *GetNewCidReturnType, genWG *sync.WaitGroup) {
	genWG.Add(1)
	defer genWG.Done()
	for elem := range GetNewCidReturnTypeChannel {
		fmt.Print("received new element: ")
		fmt.Println(elem)
	}
}
