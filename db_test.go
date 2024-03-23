package scache

import (
	"strconv"
	"strings"
	"testing"
)

func TestUint32(t *testing.T) {
	names := [...]string{"0000.scl", "0001.scl", "0002.scl", "0003.scl"}
	var fileIds []uint32
	for _, name := range names {
		if strings.HasSuffix(name, "scl") {
			temp := strings.Split(name, ".")[0]
			id, err := strconv.Atoi(temp)
			if err != nil {
				panic("data file name error,filename should be like 0001.scl")
			}
			fileIds = append(fileIds, uint32(id))
		}
	}
	t.Log(fileIds)
}
