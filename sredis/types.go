package sredis

import (
	"errors"
	"github.com/sharch/scache"
)

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

type redisDataType = byte

const (
	String redisDataType = iota
	Hash
	Set
	List
	ZSet
)

// RedisDataStructure Redis 数据结构服务
type RedisDataStructure struct {
	db *scache.DB
}

// NewRedisDataStructure 初始化 Redis 数据结构服务
func NewRedisDataStructure(options scache.Options) (*RedisDataStructure, error) {
	db, err := scache.Open(options)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{db: db}, nil
}

func (rds *RedisDataStructure) Close() error {
	return rds.db.Close()
}
