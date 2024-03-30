package scache

import (
	"errors"
	"github.com/sharch/scache/data"
	"github.com/sharch/scache/fio"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const MergeFinKey = "$1?_merge_fin"

// Merge 数据优化，去除已经删除的数据
func (d *DB) Merge() error {
	if d.activeFile == nil {
		return nil
	}
	d.mu.Lock()
	if d.isMerging {
		d.mu.Unlock()
		return errors.New("already in merge processing,try later")
	}
	d.isMerging = true
	defer func() {
		d.isMerging = false
	}()
	if err := d.activeFile.Sync(); err != nil {
		d.mu.Unlock()
		return err
	}
	// 1. 当前的活跃文件转为old文件
	d.oldFiles[d.activeFile.FileId] = d.activeFile
	// 2. 创建新活跃文件
	if err := d.newActiveFile(); err != nil {
		d.mu.Unlock()
		return err
	}
	// 3. 拿到所有需要merge处理的文件
	var mergeFiles []*data.LogDataFile
	for _, file := range d.oldFiles {
		mergeFiles = append(mergeFiles, file)
	}
	d.mu.Unlock()
	// 4. 依次merge处理
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})
	// 4.1 创建临时merge目录
	mergePath := d.makeMergeDirPath()
	// 4.2 目录已经存在就删除重建
	if _, err := os.Stat(mergePath); err != nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	// 5. 新建临时merge实例
	mergeOption := d.Options
	mergeOption.DirPath = mergePath
	mergeOption.SafeWrite = false
	tempMergeDB, err := OpenDB(mergeOption)
	if err != nil {
		return err
	}
	// 6. 开始重新数据
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			dataRecord, err := dataFile.ReadLogRecord(offset)
			size := dataRecord.Size
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			realKey, _ := data.ParseLogRecordKey(dataRecord.Key)
			// 拿到这个key对于的最新数据位置
			pos := d.index.Get(realKey)
			if pos != nil && pos.Fid == dataFile.FileId && pos.Offset == offset {
				dataRecord.Key = joinKeyWithSeq(NonTxnSeq, realKey)
				newPos, err := tempMergeDB.appendLogRecord(dataRecord)
				if err != nil {
					return err
				}
				if err := hintFile.AppendHintRecord(realKey, newPos); err != nil {
					return err
				}
			}
			offset += size
		}
	}
	// 7. 持久化文件
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := tempMergeDB.Sync(); err != nil {
		return err
	}
	// 8. merge结束标记
	finishedMarkFile, err := MergeFinishedMark(mergePath)
	if err != nil {
		return err
	}
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(MergeFinKey),
		Value: []byte(strconv.Itoa(int(d.activeFile.FileId))),
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := finishedMarkFile.Write(encRecord); err != nil {
		return err
	}
	if err := finishedMarkFile.Sync(); err != nil {
		return err
	}
	return nil
}

// makeMergeDirPath 创建临时使用的merge文件夹
func (d *DB) makeMergeDirPath() string {
	dir := path.Dir(path.Clean(d.Options.DirPath))
	baseDir := path.Base(d.Options.DirPath)
	return filepath.Join(dir, baseDir+"_merge")
}

// MergeFinishedMark merge结束标记
func MergeFinishedMark(dirPath string) (*data.LogDataFile, error) {
	fileName := filepath.Join(dirPath, data.MERGE_DONE_MARK_FILE_NAME)
	// 初始化IOManager
	manager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &data.LogDataFile{
		FileId:    0,
		Pos:       0,
		IOManager: manager,
	}, nil
}

func (db *DB) loadMergedFiles() error {
	dirPath := db.makeMergeDirPath()
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(dirPath)
	}()
	dirItems, err := os.ReadDir(dirPath)
	if err != nil {
		return err
	}

	// 查找标识 merge 完成的文件，判断 merge 是否处理完了
	var mergeFinished bool
	var mergeFileNames []string
	for _, entry := range dirItems {
		if entry.Name() == data.MERGE_DONE_MARK_FILE_NAME {
			mergeFinished = true
		}
		if entry.Name() == data.Seq_No_File_Name {
			continue
		}
		if entry.Name() == data.FileLockedName {
			continue
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	if !mergeFinished {
		return nil
	}
	nonMergeFileId, err := db.getNonMergeFileId(dirPath)
	if err != nil {
		return nil
	}
	// 删除旧的数据文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.Options.DirPath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}
	// 将新的数据文件移动到数据目录中
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(dirPath, fileName)
		destPath := filepath.Join(db.Options.DirPath, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	record, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil
}

// 从 hint 文件中加载索引
func (db *DB) loadIndexFromHintFile() error {
	// 查看 hint 索引文件是否存在
	hintFileName := filepath.Join(db.Options.DirPath, data.HINT_FILE_NAME)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	//	打开 hint 索引文件
	hintFile, err := data.OpenHintFile(db.Options.DirPath)
	if err != nil {
		return err
	}

	// 读取文件中的索引
	var offset int64 = 0
	for {
		logRecord, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// 解码拿到实际的位置索引
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)
		offset += logRecord.Size
	}
	return nil
}
