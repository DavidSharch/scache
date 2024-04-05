package main

import (
	"github.com/sharch/scache"
	"github.com/sharch/scache/sredis"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

const addr = "127.0.0.1:6380"

type RedisServer struct {
	dbs    map[int]*sredis.RedisDataStructure
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {
	// 打开 Redis 数据结构服务
	redisDataStructure, err := sredis.NewRedisDataStructure(scache.DefaultOptions)
	if err != nil {
		panic(err)
	}

	// 初始化 srv
	srv := &RedisServer{
		dbs: make(map[int]*sredis.RedisDataStructure),
	}
	srv.dbs[0] = redisDataStructure

	// 初始化一个 Redis 服务端
	srv.server = redcon.NewServer(addr, execClientCommand, srv.accept, srv.close)
	srv.listen()
}

func (svr *RedisServer) listen() {
	log.Println("server running, ready to accept connections.")
	_ = svr.server.ListenAndServe()
}

func (svr *RedisServer) accept(conn redcon.Conn) bool {
	cli := new(RedisClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.server = svr
	cli.db = svr.dbs[0]
	conn.SetContext(cli)
	return true
}

func (svr *RedisServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}
	_ = svr.server.Close()
}
