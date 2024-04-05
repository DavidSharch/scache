package sredis

import (
	"github.com/sharch/scache"
	"github.com/sharch/scache/utils"
	"time"
)

func (rds *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	// 构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}

	var exist = true
	// 查看是否已经存在
	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil && err != scache.ErrKeyNotFound {
		return false, err
	}
	if err == scache.ErrKeyNotFound {
		exist = false
	}
	if exist {
		if score == utils.FloatFromBytes(value) {
			return false, nil
		}
	}

	// 更新元数据和数据
	wb := rds.db.NewWriteBatch(scache.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	if exist {
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.FloatFromBytes(value),
		}
		_ = wb.Delete(oldKey.encodeWithScore())
	}
	_ = wb.Put(zk.encodeWithMember(), utils.Float64ToBytes(score))
	_ = wb.Put(zk.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (rds *RedisDataStructure) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, nil
	}

	// 构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.FloatFromBytes(value), nil
}

func (rds *RedisDataStructure) findMetadata(key []byte, dataType redisDataType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && err != scache.ErrKeyNotFound {
		return nil, err
	}

	var meta *metadata
	var exist = true
	if err == scache.ErrKeyNotFound {
		exist = false
	} else {
		meta = decodeMetadata(metaBuf)
		// 判断数据类型
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
		// 判断过期时间
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}
