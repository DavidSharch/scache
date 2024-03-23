package scache

import (
	"github.com/sharch/scache/data"
	"github.com/sharch/scache/index"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	mu         *sync.RWMutex
	activeFile *data.LogDataFile            // activeFile 只有一个活跃文件
	oldFiles   map[uint32]*data.LogDataFile // oldFiles 旧文件，文件编号->文件
	index      index.Indexer                // index 内存索引
	fileIds    []int                        // fileIds 排序后的文件id，最大的id是活跃文件
	Options
}

// ------------------------------
// -----------DB相关-------------
// ------------------------------

func OpenDB(opts Options) (*DB, error) {
	if err := CheckOptions(opts); err != nil {
		return nil, err
	}
	db := &DB{
		mu:       new(sync.RWMutex),
		Options:  opts,
		oldFiles: make(map[uint32]*data.LogDataFile),
		index:    index.NewIndexer(opts.MemoryIndexType),
	}
	// 加载数据文件
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}
	// 加载索引
	if err := db.loadIndexDataFromFile(); err != nil {
		return nil, err
	}
	return db, nil
}

// loadDataFile 加载目录下的全部数据文件
func (d *DB) loadDataFile() error {
	dirEntries, err := os.ReadDir(d.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), "scl") {
			temp := strings.Split(entry.Name(), ".")[0]
			id, err := strconv.Atoi(temp)
			if err != nil {
				panic("data file name error,filename should be like 0001.scl")
			}
			fileIds = append(fileIds, int(id))
		}
	}
	// 按照fileId顺序，从小到达依次加载文件
	sort.Ints(fileIds)
	d.fileIds = fileIds
	for i := 0; i < len(fileIds); i++ {
		fid := uint32(fileIds[i])
		file, err := data.OpenLogDataFile(d.Options.DirPath, fid)
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			// 最后一个文件是活跃文件
			d.activeFile = file
		} else {
			d.oldFiles[fid] = file
		}
	}
	return nil
}

// loadIndexDataFromFile 加载索引数据
func (d *DB) loadIndexDataFromFile() error {
	if len(d.fileIds) == 0 {
		// 最开始，可能还没有写入任何数据
		return nil
	}
	// 取出全部文件内容，构建内存索引
	// FIXME 从文件中构建索引有Bug，只能读取出一条数据
	for i, fileId := range d.fileIds {
		var dataFile *data.LogDataFile
		if uint32(fileId) == d.activeFile.FileId {
			// 活跃文件
			dataFile = d.activeFile
		} else {
			dataFile = d.oldFiles[uint32(fileId)]
		}
		var offset int64 = 0
		// 开始读取文件中的每一行
		for {
			// 拿到一条log数据
			log, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 构建内存索引数据
			key := log.Key
			logPos := &data.LogRecordPos{Fid: uint32(fileId), Offset: offset}
			if log.Type == data.LogRecordDeleted {
				d.index.Delete(key)
			} else {
				d.index.Put(key, logPos)
			}
			offset += log.Size
		}
		// 更新活跃文件的最新offset
		if i == len(d.fileIds)-1 {
			d.activeFile.Pos = offset
		}
	}
	return nil
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
		if err := d.newActiveFile(); err != nil {
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
	if ok := d.index.Put(key, pos); !ok {
		// 这里不可能更新/写入失败
		return false, ErrUpdateIndexFailed
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
	logRecord, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrDataDeleted
	}
	return logRecord.Value, nil
}

// ------------------------------
// -----------删除流程------------
// ------------------------------

func (d *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyNotValid
	}
	exists := d.index.Get(key)
	if exists == nil {
		return ErrKeyNotExists
	}
	record := &data.LogRecord{
		Key:   key,
		Value: []byte{},
		Type:  data.LogRecordDeleted,
	}
	_, err := d.appendLogRecord(record)
	if err != nil {
		return err
	}
	// 删除索引值
	if ok := d.index.Delete(key); !ok {
		return ErrUpdateIndexFailed
	}
	return nil
}
