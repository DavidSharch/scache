package scache

import "errors"

var (
	ErrKeyNotValid = errors.New("key is not a valid string")
	ErrUpdateIndex = errors.New("update index error")
)
