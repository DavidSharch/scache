package fio

const FilePerm = 0644

// IOManager IO操作。可以使用标准IO、mmap等实现
type IOManager interface {
	// Read 从文件指定位置读取数据，把数据写入data中
	Read(data []byte, offset int64) (int, error)
	// Write 写入字节数组到文件中
	Write(data []byte) (int, error)
	// Sync 刷盘
	Sync() error
	Close() error
}
