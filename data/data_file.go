package data

import (
	"errors"
	"fmt"
	"github.com/sharch/scache/fio"
	"hash/crc32"
	"io"
	"path/filepath"
)

const DataFileNameSuffix = ".scl"

type LogDataFile struct {
	FileId    uint32
	Pos       int64 // 当前文件写入offset
	IOManager fio.IOManager
}

func (f *LogDataFile) Close() error {
	return f.IOManager.Close()
}

func (f *LogDataFile) Sync() error {
	return f.IOManager.Sync()
}

// Write 写数据，更新pos数据
func (f *LogDataFile) Write(value []byte) error {
	offset, err := f.IOManager.Write(value)
	if err != nil {
		return err
	}
	f.Pos += int64(offset)
	return nil
}

// ReadLogRecord 读取出指定offset的数据
func (f *LogDataFile) ReadLogRecord(offset int64) (*LogRecord, error) {
	fileSize, err := f.IOManager.Size()
	if err != nil {
		return nil, err
	}
	// headerLen 防止读取超过文件大小
	var headerLen int64 = MaxHeaderSize
	if offset+MaxHeaderSize > fileSize {
		headerLen = fileSize - offset
	}
	// 读取header信息
	bytes, err := f.readBytes(headerLen, offset)
	if err != nil {
		return nil, err
	}
	header, headerSize := parseHeader(bytes)
	if header == nil {
		return nil, io.EOF
	}
	if header.Crc == 0 && header.KeySize == 0 && header.ValueSize == 0 {
		return nil, io.EOF
	}
	keySize, valueSize := header.KeySize, header.ValueSize
	res := &LogRecord{
		Size: int64(keySize + valueSize),
	}
	readBytes, err := f.readBytes(res.Size, offset+headerSize)
	if err != nil {
		return nil, err
	}
	res.Key = readBytes[:keySize]
	res.Value = readBytes[keySize:]
	// CRC校验
	crc := getLogRecordCrc(res, bytes[crc32.Size:headerSize])
	if crc != header.Crc {
		// TODO 解决依赖问题
		return nil, errors.New("crc error,data is broken")
	}
	return res, nil
}

func (f *LogDataFile) readBytes(len int64, offset int64) ([]byte, error) {
	b := make([]byte, len)
	_, err := f.IOManager.Read(b, offset)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// OpenLogDataFile 打开数据文件
func OpenLogDataFile(dirPath string, fileId uint32) (*LogDataFile, error) {
	fileName := fmt.Sprintf("%09d", fileId) + DataFileNameSuffix
	fileName = filepath.Join(dirPath, fileName)
	// 初始化IOManager
	manager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &LogDataFile{
		FileId:    fileId,
		Pos:       0,
		IOManager: manager,
	}, nil
}
