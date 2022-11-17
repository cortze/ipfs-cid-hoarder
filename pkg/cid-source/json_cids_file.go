package cid_source

//A container for the encapsulated struct.
//
//File containts a json array of provider records.
//[{ProviderRecord1},{ProviderRecord2},{ProviderRecord3}]
type ProviderRecords struct {
	EncapsulatedJSONProviderRecords []EncapsulatedJSONProviderRecord `json:"ProviderRecords"`
}

//This struct will be used to create,read and store the encapsulated data necessary for reading the
//provider records.
type EncapsulatedJSONProviderRecord struct {
	ID          string   `json:"PeerID"`
	CID         string   `json:"ContentID"`
	Creator     string   `json:"Creator"`
	ProvideTime string   `json:"ProvideTime"`
	UserAgent   string   `json:"UserAgent"`
	Addresses   []string `json:"PeerMultiaddresses"`
}

//Creates a new:
//	EncapsulatedCidProvider struct {
//		ID      string
//		CID     string
//		Address ma.Multiaddr
//	}
func NewEncapsulatedJSONCidProvider(id string, cid string, addresses []string, creator string, providetime string, useragent string) EncapsulatedJSONProviderRecord {
	return EncapsulatedJSONProviderRecord{
		ID:          id,
		CID:         cid,
		Creator:     creator,
		ProvideTime: providetime,
		UserAgent:   useragent,
		Addresses:   addresses,
	}
}

const filename = "providers.json"
