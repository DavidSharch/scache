package scache

import "errors"

var (
	ErrKeyNotValid       = errors.New("key is not a valid string")
	ErrUpdateIndexFailed = errors.New("update index error")
	ErrKeyNotExists      = errors.New("key is not exists")
	ErrDataFileNotFound  = errors.New("data file not found")
	ErrDataDeleted       = errors.New("data already deleted")

	// DB 相关
	ErrDirNotExisted = errors.New("db dir path not exists")
	ErrFileTooSmall  = errors.New("db file size <= 0")
)
