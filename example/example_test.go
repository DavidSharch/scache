package main

import (
	"fmt"
	"testing"
	"time"
)

func TestName(t *testing.T) {
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("test_%d", i)
		value := fmt.Sprintf("%s", time.Now())
		t.Log(key)
		t.Log(value)
	}
}
