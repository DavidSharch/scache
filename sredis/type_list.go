package sredis

import "github.com/sharch/scache"

func (rds *RedisDataStructure) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

func (rds *RedisDataStructure) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

func (rds *RedisDataStructure) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

func (rds *RedisDataStructure) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

func (rds *RedisDataStructure) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	// 构造数据部分的 key
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	// 更新元数据和数据部分
	wb := rds.db.NewWriteBatch(scache.DefaultWriteBatchOptions)
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	_ = wb.Put(key, meta.encode())
	_ = wb.Put(lk.encode(), element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}

	return meta.size, nil
}

func (rds *RedisDataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	// 构造数据部分的 key
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}

	element, err := rds.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	// 更新元数据
	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}
	if err = rds.db.Put(key, meta.encode()); err != nil {
		return nil, err
	}

	return element, nil
}
