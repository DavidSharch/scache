package data

import (
	"github.com/sharch/scache/fio"
	"path/filepath"
)

const HINT_FILE_NAME = "scache_hint_file"
const MERGE_DONE_MARK_FILE_NAME = "merge_done_mark"
const Seq_No_File_Name = "seq-no"

// OpenHintFile 创建hint文件
func OpenHintFile(dirPath string) (*LogDataFile, error) {
	fileName := filepath.Join(dirPath, HINT_FILE_NAME)
	// 初始化IOManager
	manager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &LogDataFile{
		FileId:    0,
		Pos:       0,
		IOManager: manager,
	}, nil
}

func (f *LogDataFile) AppendHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encRecord, _ := EncodeLogRecord(record)
	return f.Write(encRecord)
}
