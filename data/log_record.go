package data

// LogRecordPos 数据保存在哪个文件的哪个位置
type LogRecordPos struct {
	Fid    uint32 // Fid 保存的文件id
	Offset int64  // Pos 保存位置，类似kafka
}

type LogRecordType = byte

const (
	LogRecordNormal  LogRecordType = iota
	LogRecordDeleted LogRecordType = iota
)

const MaxHeaderSize = 15

// LogRecord 保存数据
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
	Size  int64 // LogRecord 所占byte长度
}

type LogRecordHeader struct {
	Crc       uint32        // Crc 校验
	Type      LogRecordType // Type 类型
	KeySize   uint32        // KeySize key长度，可变
	ValueSize uint32        // ValueSize key长度，可变
}

func EncodeLogRecord(record *LogRecord) (value []byte, valueSize int64) {
	return nil, 0
}

func parseHeader(b []byte) (*LogRecordHeader, int64) {
	return nil, 0
}
