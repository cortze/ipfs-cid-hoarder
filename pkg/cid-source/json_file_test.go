package cid_source

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
)

//Tests if json file opens properly and the json contents are read
func TestOpenEncodedJSONFile(t *testing.T) {
	filename := "examplejsonfiles\\providersen.json"
	file, err := OpenEncodedJSONFile(filename)
	if err != nil {
		t.Errorf("error %s while trying to open simple json file", err)
		return
	}

	for i := 0; i < len(file.records.EncapsulatedJSONProviderRecords); i++ {
		fmt.Println(file.records.EncapsulatedJSONProviderRecords[i])
	}

}

func TestOpenMultipleEncodedJSONFiles(t *testing.T) {
	filenames := make([]string, 0)
	filenames = append(filenames, "examplejsonfiles\\encodedQma2PDpcZX6jY88fqWeFvzw5HseGvumi7RJzQ73ie9JZ7wproviders.json")
	filenames = append(filenames, "examplejsonfiles\\encodedQmc5bTVHfwdTyi2anYg5XvNwiBqZpbCsmDtVau1CmBYhmLproviders.json")
	filenames = append(filenames, "examplejsonfiles\\encodedQmcjTjQWn7p1r9j9JWgkevkakqEoLXLbaJj2EzeEH4dEJyproviders.json")
	filenames = append(filenames, "examplejsonfiles\\encodedQmcWB1SwVDkDuLhv5UGJhhN3wQ1vaCoKWfetvoqGMQyh7Qproviders.json")
	file, err := OpenMultipleEncodedJSONFiles(filenames)
	if err != nil {
		t.Errorf("error %s while opening json files", err)
		return
	}
	for _, rec := range file.records.EncapsulatedJSONProviderRecords {
		fmt.Println(rec)
	}
}

func TestOpenSimpleJSONFile(t *testing.T) {
	filename := "examplejsonfiles\\providers.json"
	file, err := OpenSimpleJSONFile(filename)
	if err != nil {
		t.Errorf("error %s while trying to open simple json file", err)
		return
	}
	for _, rec := range file.records.EncapsulatedJSONProviderRecords {
		fmt.Println(rec)
	}
}

func TestOpenMultipleSimpleJSONFiles(t *testing.T) {
	filenames := make([]string, 0)
	filenames = append(filenames, "examplejsonfiles\\Qma2PDpcZX6jY88fqWeFvzw5HseGvumi7RJzQ73ie9JZ7wproviders.json")
	filenames = append(filenames, "examplejsonfiles\\Qmc5bTVHfwdTyi2anYg5XvNwiBqZpbCsmDtVau1CmBYhmLproviders.json")
	filenames = append(filenames, "examplejsonfiles\\QmcjTjQWn7p1r9j9JWgkevkakqEoLXLbaJj2EzeEH4dEJyproviders.json")
	filenames = append(filenames, "examplejsonfiles\\QmcWB1SwVDkDuLhv5UGJhhN3wQ1vaCoKWfetvoqGMQyh7Qproviders.json")
	file, err := OpenMultipleSimpleJSONFiles(filenames)
	if err != nil {
		t.Errorf("error %s while opening json files", err)
		return
	}
	for _, rec := range file.records.EncapsulatedJSONProviderRecords {
		fmt.Println(rec)
	}
}

func TestGetNewCidSimpleJSON(t *testing.T) {
	file, err := OpenSimpleJSONFile("examplejsonfiles\\providers.json")
	if err != nil {
		return
	}
	for true {
		tp, err := file.GetNewCid()
		if reflect.DeepEqual(tp, TrackableCid{}) {
			break
		}
		if err != nil {
			t.Errorf("error %s while getting new cid", err)
			return
		}

	}
}

func TestGetNewCidEncodedJSON(t *testing.T) {
	file, err := OpenEncodedJSONFile("examplejsonfiles\\providersen.json")
	if err != nil {
		t.Errorf("error %s while opening file", err)
		return
	}
	for true {
		tp, err := file.GetNewCid()
		if reflect.DeepEqual(tp, TrackableCid{}) {
			break
		}
		if err != nil {
			t.Errorf("error %s while getting new cid", err)
			return
		}

	}
}

func TestJsonFileCIDSource_GetNewCidWithChannel(t *testing.T) {
	TrackableCidChannel := make(chan *TrackableCid, 10)
	var genWG sync.WaitGroup
	go routineForReceivingFromChannel(TrackableCidChannel, &genWG)
	file, err := OpenSimpleJSONFile("examplejsonfiles\\providers.json")
	if err != nil {
		t.Errorf("error %s while opening json file", err)
	}
	for true {
		cid, err := file.GetNewCid()
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
}
