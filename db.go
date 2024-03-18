package scache

import (
	"github.com/sharch/scache/data"
	"github.com/sharch/scache/index"
	"sync"
)

type DB struct {
	mu         *sync.RWMutex
	activeFile *data.LogDataFile            // activeFile 只有一个活跃文件
	oldFiles   map[uint32]*data.LogDataFile // oldFiles旧文件，文件编号->文件
	index      index.Indexer                // index 内存索引
	Options
}

// ------------------------------
// -----------写入流程------------
// ------------------------------

// appendLogRecord 追加数据，返回数据的pos位置
func (d *DB) appendLogRecord(record *data.LogRecord) (*data.LogRecordPos, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	// active file是否存在
	if d.activeFile == nil {
		if err := d.newActiveFile(); nil != nil {
			return nil, err
		}
	}
	value, size := data.EncodeLogRecord(record)
	// 判断文件是否可以继续写入
	if d.activeFile.Pos+size > d.Options.MaxSize {
		pos, err := d.makeNewActiveFileAndSync()
		if err != nil {
			return pos, err
		}
	}
	// 数据写入活跃文件
	curOffset := d.activeFile.Pos
	err := d.activeFile.Write(value)
	if err != nil {
		return nil, err
	}
	curOffset += size
	if d.SafeWrite {
		err := d.activeFile.Sync()
		if err != nil {
			return nil, err
		}
	}
	pos := &data.LogRecordPos{
		Fid:    d.activeFile.FileId,
		Offset: curOffset,
	}
	return pos, err
}

// makeNewActiveFileAndSync 当前活跃文件刷盘，然后创建出新活跃文件
func (d *DB) makeNewActiveFileAndSync() (*data.LogRecordPos, error) {
	// sync刷盘
	if err := d.activeFile.Sync(); err != nil {
		return nil, err
	}
	// 当前活跃文件失活
	d.oldFiles[d.activeFile.FileId] = d.activeFile
	// 创建新活跃文件
	if err := d.newActiveFile(); err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *DB) Put(key []byte, value []byte) (bool, error) {
	if key == nil || len(key) == 0 {
		return false, ErrKeyNotValid
	}

	dataLog := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	pos, err := d.appendLogRecord(dataLog)
	if err != nil {
		return false, nil
	}
	// 更新内存索引
	if ok := d.index.Put(key, pos); ok {
		return false, ErrUpdateIndex
	}
	return true, nil
}

func (d *DB) newActiveFile() error {
	var fileId uint32 = 0
	if d.activeFile != nil {
		fileId = d.activeFile.FileId + 1
	}
	file, err := data.OpenLogDataFile(d.DirPath, fileId)
	if err != nil {
		return err
	}
	d.activeFile = file
	return nil
}

// ------------------------------
// -----------读取流程------------
// ------------------------------

// Get 读取数据，使用读写锁保护
func (d *DB) Get(key []byte) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if key == nil || len(key) == 0 {
		return nil, ErrKeyNotValid
	}
	// 从内存中读取到所在文件和位置
	pos := d.index.Get(key)
	if pos == nil {
		return nil, ErrKeyNotExists
	}
	// 数据在活跃文件中
	var dataFile *data.LogDataFile
	if d.activeFile.FileId == pos.Fid {
		dataFile = d.activeFile
	} else {
		dataFile = d.oldFiles[pos.Fid]

	}
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	logRecord, err := dataFile.ReadData(pos.Offset)
	if err != nil {
		return nil, err
	}
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrDataDeleted
	}
	return logRecord.Value, nil
}
