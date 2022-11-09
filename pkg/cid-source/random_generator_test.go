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
	for {
		cid, err := randomgen.GetNewCid()
		if reflect.DeepEqual(cid, TrackableCid{}) {
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
	TrackableCidChannel := make(chan *TrackableCid, 10)
	var genWG sync.WaitGroup
	go routineForReceivingFromChannel(TrackableCidChannel, &genWG)
	randomgen := NewRandomCidGen(6, 8)
	i := 0
	for {
		cid, err := randomgen.GetNewCid()
		if reflect.DeepEqual(cid, TrackableCid{}) {
			break
		}
		if err != nil {
			t.Errorf("error %s while generating random cid", err)
		}
		TrackableCidChannel <- &cid
	}
	close(TrackableCidChannel)
	genWG.Wait()
	if i > randomgen.limit {
		t.Errorf("i bigger than limit")
	}
}

func routineForReceivingFromChannel(TrackableCidChannel <-chan *TrackableCid, genWG *sync.WaitGroup) {
	genWG.Add(1)
	defer genWG.Done()
	for elem := range TrackableCidChannel {
		fmt.Print("received new element: ")
		fmt.Println(elem)
	}
}
