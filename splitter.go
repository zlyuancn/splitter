package splitter

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
)

const (
	MinChunkSizeLimit        = 16
	MinValueMaxScanSizeLimit = 4096
)

type FlushChunkArgs struct {
	ChunkSn     int    // chunk sn
	StartDataSn int64  // 第一个数据的 sn
	EndDataSn   int64  // 最后一个数据的sn
	Data        []byte // 数据
	ScanByteNum int64  // 已扫描rd的字节数
}

// flush Chunk 函数
type FlushChunkHandler func(args *FlushChunkArgs)

// 值过滤器, 返回空字节或者nil则抛弃该value
type ValueFilter func(value []byte) []byte

type Conf struct {
	Delim                 []byte            // 分隔符
	ChunkSizeLimit        int               // chunk 长度限制, 一个chunk的长度一般会小于这个值, 但是value超出chunk长度时会作为一个chunk, 此时chunk长度会超出这个值
	FlushChunkHandler     FlushChunkHandler // flushChunk函数
	ValueMaxScanSizeLimit int               // value 最大扫描长度限制, 如果扫描一定长度还无法确认一个完整的value则返回错误
	ValueFilter           ValueFilter       // value过滤器
}
type splitter struct {
	chunkSizeLimit    int           // chunk长度限制
	chunkBuffer       *bytes.Buffer // chunk缓冲区
	chunkSn           int           // chunk 编号
	chunkStartDataSn  int64         // chunk 的第一个数据的 sn
	nowDataValueSn    int64         // 当前数据 sn
	flushChunkHandler FlushChunkHandler

	delimiter             []byte      // 分隔符
	valueMaxScanSizeLimit int         // value 最大扫描长度限制
	valueFilter           ValueFilter // value过滤器

	started int32 // 是否已启动
	stopped int32 // 是否已停止
}

func NewSplitter(conf Conf) *splitter {
	if len(conf.Delim) == 0 {
		panic("delim must not be empty")
	}
	s := &splitter{
		chunkSizeLimit:    max(conf.ChunkSizeLimit, MinChunkSizeLimit),
		chunkBuffer:       bytes.NewBuffer(make([]byte, 0, conf.ChunkSizeLimit)),
		chunkSn:           0,
		chunkStartDataSn:  0,
		nowDataValueSn:    -1,
		flushChunkHandler: conf.FlushChunkHandler,

		delimiter:             conf.Delim,
		valueMaxScanSizeLimit: max(conf.ValueMaxScanSizeLimit, MinValueMaxScanSizeLimit),
		valueFilter:           conf.ValueFilter,
	}
	if s.flushChunkHandler == nil {
		s.flushChunkHandler = defaultFlushChunkHandler
	}
	return s
}

// 分隔
func (s *splitter) Split(rd io.Reader) error {
	// 防止重复调用
	if atomic.AddInt32(&s.started, 1) != 1 {
		return errors.New("Repeat Call Split")
	}

	// 创建值读取器
	vr := NewValueReader(rd, s.delimiter, s.valueMaxScanSizeLimit)

	for {
		if atomic.LoadInt32(&s.stopped) > 0 {
			return nil
		}

		value, err := vr.Next() // 获取下一个值
		if err != nil && err != io.EOF {
			return err
		}

		if s.valueFilter != nil && len(value) > 0 {
			value = s.valueFilter(value)
		}

		if len(value) > 0 {
			// 如果加入这个 value 会超过 限制，则先 flush 当前 chunk
			if s.chunkBuffer.Len()+len(value) > s.chunkSizeLimit && s.chunkBuffer.Len() > 0 {
				chunkSn := s.chunkSn
				s.chunkSn++
				s.flushChunk(&FlushChunkArgs{
					ChunkSn:     chunkSn,
					StartDataSn: s.chunkStartDataSn,
					EndDataSn:   s.nowDataValueSn,
					Data:        s.chunkBuffer.Bytes(),
					ScanByteNum: vr.GetScanByteNum(),
				})
				s.chunkBuffer.Reset()
				s.chunkStartDataSn = s.nowDataValueSn + 1
			}

			s.chunkBuffer.Write(value)
			s.chunkBuffer.Write(s.delimiter) // 写入值后要写入分隔符
			s.nowDataValueSn++
		}

		// 在 EOF 时处理最后一个 chunk
		if err == io.EOF {
			if s.chunkBuffer.Len() > 0 {
				chunkSn := s.chunkSn
				s.chunkSn++
				s.flushChunk(&FlushChunkArgs{
					ChunkSn:     chunkSn,
					StartDataSn: s.chunkStartDataSn,
					EndDataSn:   s.nowDataValueSn,
					Data:        s.chunkBuffer.Bytes(),
					ScanByteNum: vr.GetScanByteNum(),
				})
				s.chunkBuffer.Reset()
			}
			break
		}
	}
	return nil
}

func (s *splitter) flushChunk(args *FlushChunkArgs) {
	// 这里目的是为了去掉chunk中最后的分隔符
	src := args.Data[:len(args.Data)-len(s.delimiter)]
	bs := make([]byte, len(src))

	// 创建副本
	copy(bs, src)

	s.flushChunkHandler(args)
}

func (s *splitter) Stop() {
	atomic.AddInt32(&s.stopped, 1)
}

func defaultFlushChunkHandler(args *FlushChunkArgs) {
	fmt.Println(args.ChunkSn, args.StartDataSn, args.EndDataSn, string(args.Data))
}
