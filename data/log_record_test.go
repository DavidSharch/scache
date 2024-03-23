package data

import "testing"

func TestEncodeLogRecord(t *testing.T) {
	log1 := &LogRecord{
		Key:   []byte("test1"),
		Value: []byte("value1"),
		Type:  LogRecordNormal,
	}
	res1, size1 := EncodeLogRecord(log1)
	t.Log(res1)
	t.Log(size1)
}

func TestDecodeLogRecordHeader(t *testing.T) {
	log1 := &LogRecord{
		Key:   []byte("test1"),
		Value: []byte("value1"),
		Type:  LogRecordNormal,
	}
	res1, _ := EncodeLogRecord(log1)
	header, _ := parseHeader(res1)
	t.Log(header)
}
