package scache

import "errors"

var (
	ErrKeyNotValid      = errors.New("key is not a valid string")
	ErrUpdateIndex      = errors.New("update index error")
	ErrKeyNotExists     = errors.New("key is not exists")
	ErrDataFileNotFound = errors.New("data file not found")
	ErrDataDeleted      = errors.New("data already deleted")
)
