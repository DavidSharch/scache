package data

import "github.com/sharch/scache/fio"

type LogDataFile struct {
	FileId    uint32
	Pos       int64 // 当前文件写入offset
	IOManager fio.IOManager
}

func (f *LogDataFile) Sync() error {
	return nil
}

func (f *LogDataFile) Write(value []byte) error {
	return nil
}

// OpenLogDataFile 打开数据文件
func OpenLogDataFile(dirPath string, fileId uint32) (*LogDataFile, error) {
	return nil, nil
}
