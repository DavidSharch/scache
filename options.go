package scache

type Options struct {
	DirPath   string // DirPath 数据文件保存位置
	Port      uint   // Port 端口
	MaxSize   int64  // MaxSize 文件最大byte size
	SafeWrite bool   // WriteSafe 是否每次写入数据后，都刷盘
}
