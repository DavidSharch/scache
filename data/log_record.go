package data

// LogRecordPos 数据保存在哪个文件的哪个位置
type LogRecordPos struct {
	Fid    uint // Fid 保存的文件id
	Offset uint // Pos 保存位置，类似kafka
}
