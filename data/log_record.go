package data

import (
	"encoding/binary"
	"hash/crc32"
)

// LogRecordPos 数据保存在哪个文件的哪个位置
type LogRecordPos struct {
	Fid    uint32 // Fid 保存的文件id
	Offset int64  // Pos 保存位置，类似kafka
}

type LogRecordType = byte

const (
	LogRecordNormal  LogRecordType = iota
	LogRecordDeleted LogRecordType = iota
	LogRecordTxnFin  LogRecordType = iota
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

type TxnRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// EncodeLogRecord 将kv编码
func EncodeLogRecord(record *LogRecord) (value []byte, valueSize int64) {
	header := make([]byte, MaxHeaderSize)
	// 5为type
	header[4] = record.Type
	index := 5
	// keySize
	index += binary.PutVarint(header[index:], int64(len(record.Key)))
	// valueSize
	index += binary.PutVarint(header[index:], int64(len(record.Value)))

	valueSize = int64(index + len(record.Key) + len(record.Value))
	value = make([]byte, valueSize)

	// 拷贝header，key和value
	copy(value[:index], header[:index])
	copy(value[index:], record.Key)
	copy(value[index+len(record.Key):], record.Value)

	// 0~4 为crc
	crc := crc32.ChecksumIEEE(value[4:])
	//print(fmt.Sprintf("crc:%d", crc))
	binary.LittleEndian.PutUint32(value[:4], crc)

	return value, valueSize
}

// parseHeader 解析header
func parseHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &LogRecordHeader{
		Crc:  binary.LittleEndian.Uint32(buf[:4]),
		Type: buf[4],
	}

	var index = 5
	// 取出实际的 key size
	keySize, n := binary.Varint(buf[index:])
	header.KeySize = uint32(keySize)
	index += n

	// 取出实际的 value size
	valueSize, n := binary.Varint(buf[index:])
	header.ValueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// getLogRecordCrc 计算CRC
func getLogRecordCrc(l *LogRecord, header []byte) uint32 {
	if l == nil {
		return 0
	}
	// 计算header+key+value数据下的crc
	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, l.Key)
	crc = crc32.Update(crc, crc32.IEEETable, l.Value)
	return crc
}
