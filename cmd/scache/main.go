package main

import (
	"fmt"
	"time"
)

func main() {
	print("hello from scache")
	fmt.Printf("hello from scache->:%s", time.Now().Format("2006-01-02 15:04:05"))
}
