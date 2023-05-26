package db

import (
	"fmt"
	"testing"
)

func TestUtils (t *testing.T) {
	query := `
		INSERT INTO peer_info(
			peer_id,
			cid_hash)`

	newQuery := multiValueComposer(
		query,
		"ON CONFLICT DO NOTHING",
		4,
		2)
	fmt.Println(query)
	fmt.Println(newQuery)
}
