package scache

import (
	"github.com/sharch/scache/index"
	"os"
)

type Options struct {
	DirPath         string                // DirPath 数据文件保存位置
	Port            uint                  // Port 端口
	MaxSize         int64                 // MaxSize 文件最大byte size
	SafeWrite       bool                  // WriteSafe 是否每次写入数据后，都刷盘
	MemoryIndexType index.MemoryIndexType // MemoryIndexType内存索引类型
}

func CheckOptions(opt Options) error {
	if len(opt.DirPath) == 0 {
		return ErrDirNotExisted
	}
	if _, err := os.Stat(opt.DirPath); os.IsNotExist(err) {
		err := os.MkdirAll(opt.DirPath, os.ModePerm)
		if err != nil {
			return err
		}
	}
	if opt.MaxSize <= 0 {
		return ErrFileTooSmall
	}
	return nil
}
