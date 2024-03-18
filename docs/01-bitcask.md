# note-1
已经有redis这个优秀的kv存储了，为什么还需要写一个新的kv存储呢？

redis是纯内存的，这里的kv是指主要依赖磁盘的存储

使用磁盘作为存储，不再受限于内存大小

kv的数据结构主要是B+树（ETCD的boltDB），LSM树（顺序IO，写性能高。LevelDB）

## bitcask

依赖磁盘，在磁盘上一个bitcask实例就是一个目录

同一时刻只有一个进程能打开这个目录

一个目录下有多个文件，一个文件有数据量阈值，超过后就成为旧文件，后续数据写入新文件

目录下就是一个新文件和多个旧文件（数据都是追加写入，append only，顺序IO）

顺序IO，说明不能真实删除数据，是写入新的墓碑值，说明某个数据已经被删除（有merge整理过程，类似mysql表优化。merge会将所有的旧文件，整理成新的文件替换。旧文件是只读的）

为了加快旧数据的查询，merge之后，会生成Hint文件（类似mysql的普通索引，记录key的位置）

## 数据保存方式

为了方便查询数据，内存中保存key和value所在位置的map（类似索引。可以选择hash，b+，跳表）

key-->file_id，value_size，value_pos，时间戳

查询，就需要先通过key找到file_id，再通过value_pos，value_size拿到value位置

### 数据格式

数据封装

- CRC校验
- 时间戳
- key size
- value size
- key
- value

（也许可以开启压缩，使用protobuf？使用base64+zlib？需要添加标志位）

# 架构

用户调用api

-->数据写入内存（数据按照kv），内存中的结构可以由hash、跳表、b树等实现

-->落盘，再使用bitcask