package main

import (
	"fmt"
	"github.com/sharch/scache"
	"time"
)

func main() {
	opts := scache.DefaultOption
	opts.DirPath = "D:/code/scache/tmp"
	db, err := scache.OpenDB(opts)
	if err != nil {
		panic("启动失败")
	}
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("test_%d", i)
		value := fmt.Sprintf("%s", time.Now())
		_, err = db.Put([]byte(key), []byte(value))
		if err != nil {
			print(i, "\n")
			panic(err.Error())
		}
	}
	res, err := db.Get([]byte("test_13"))
	if err != nil {
		panic(err.Error())
	}
	fmt.Println("value-->", string(res))
}
