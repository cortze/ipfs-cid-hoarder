package db

import (
	"fmt"
	"strings"
)

// multiValueComposer includes the VALUES ($1, $2) ($3, $4) for the given query on the given
// valueLen and itemsPerValue and returns the new query
func multiValueComposer (mainQuery string, appendable string, valueLen, itemsPerValue int) string {
	var s strings.Builder
	s.WriteString(mainQuery)
	baseIdx := 1
	for valueItem := 0; valueItem < valueLen; valueItem++ {
		if valueItem == 0 {
			s.WriteString("\nVALUES (")
		} else {
			s.WriteString(",(")
		}
		for item := baseIdx; item < (baseIdx + itemsPerValue); item++ {
			if item != baseIdx {
				s.WriteString(",")
			}
			s.WriteString(fmt.Sprintf("$%d", item))
		}	
		s.WriteString(")")
		baseIdx += itemsPerValue
	}
	if appendable != "" {
		s.WriteString("\n")
		s.WriteString(appendable)
	}
	return s.String()
}
