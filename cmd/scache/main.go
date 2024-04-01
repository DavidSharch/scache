package main

import (
	"encoding/json"
	"fmt"
	"github.com/sharch/scache"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

var db *scache.DB

const version = "v0.0.1"

func init() {
	// 初始化 DB 实例
	options := scache.DefaultOptions
	err := os.MkdirAll("D:/code/scache/temp", 0777)
	if err != nil {
		panic(fmt.Sprintf("failed to open db: %v", err))
	}
	options.DirPath = "D:/code/scache/temp"
	db, err = scache.Open(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open db: %v", err))
	}
	if err := db.Put([]byte("hello"), []byte("world,I am scache "+version)); err != nil {
		panic(fmt.Sprintf("failed to open db: %v", err))
	}
}

func handlePut(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var kv map[string]string

	if err := json.NewDecoder(request.Body).Decode(&kv); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	for key, value := range kv {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to put kv in db: %v\n", err)
			return
		}
	}
}

func handleGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")

	value, err := db.Get([]byte(key))
	if err != nil && err != scache.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get kv in db: %v\n", err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(string(value))
}

func handleDelete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodDelete {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")

	err := db.Delete([]byte(key))
	if err != nil && err != scache.ErrKeyIsEmpty {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get kv in db: %v\n", err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("OK")
}

func handleListKeys(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	keys := db.ListKeys()
	writer.Header().Set("Content-Type", "application/json")
	var result []string
	for _, k := range keys {
		result = append(result, string(k))
	}
	_ = json.NewEncoder(writer).Encode(result)
}

func handleStat(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stat := db.Stat()
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(stat)
}

func main() {
	// 注册处理方法
	http.HandleFunc("/c/put", handlePut)
	http.HandleFunc("/c/get", handleGet)
	http.HandleFunc("/c/delete", handleDelete)
	http.HandleFunc("/c/listkeys", handleListKeys)
	http.HandleFunc("/c/stat", handleStat)
	go func() {
		// get localhost:8089/c/get?key=hello
		if err := http.ListenAndServe("localhost:8089", nil); err != nil && err != http.ErrServerClosed {
			fmt.Printf("listen: %s\n", err)
		}
	}()

	// 监听系统退出信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, []os.Signal{syscall.SIGINT, syscall.SIGTERM}...)
	fmt.Println(fmt.Sprintf("\nexit: %v \n", <-quit))
	fmt.Printf("closing db ...\n")
	err := db.Close()
	if err != nil {
		panic(fmt.Sprintf("close db failed\n"))
	}
	fmt.Printf("closing db done\n")
}
