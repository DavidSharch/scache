package fio

import "os"

// FileIO 标准文件IO，封装golang文件处理API
type FileIO struct {
	fd *os.File
}

func NewFileIO(fileName string) (*FileIO, error) {
	// 给文件权限，创建、可读可写、只能追加写
	file, err := os.OpenFile(fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		FilePerm,
	)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: file}, nil
}

func (fio *FileIO) Read(data []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(data, offset)
}

func (fio *FileIO) Write(data []byte) (int, error) {
	return fio.fd.Write(data)
}

func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIO) Close() error {
	return fio.fd.Close()
}
