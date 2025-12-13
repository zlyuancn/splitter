package splitter

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
)

var ErrSplitterIsStarted = errors.New("splitter is started")
var ErrSplitterIsStopped = errors.New("splitter is stopped")

const (
	MinChunkSizeLimit        = 16
	MinValueMaxScanSizeLimit = 4096
)

type FlushChunkArgs struct {
	ChunkSn      int    // chunk sn
	StartValueSn int64  // 第一个 value 的 sn
	EndValueSn   int64  // 最后一个 value 的 sn
	ChunkData    []byte // chunk数据
	ScanByteNum  int64  // 已扫描rd的字节数
}

// flush Chunk 函数
type FlushChunkHandler func(args *FlushChunkArgs)

// 值过滤器, 返回空字节或者nil则抛弃该value
type ValueFilter func(value []byte) []byte

type Splitter interface {
	// 从 io.Reader 中读取数据，按配置进行分片和处理。
	// 仅允许调用一次，重复调用将返回错误。
	RunSplit(rd io.Reader) error
	// 停止
	Stop()
}

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
	chunkStartValueSn int64         // chunk 的第一个 value 的 sn
	nextValueSn       int64         // 下一个 value 的 sn
	flushChunkHandler FlushChunkHandler

	delimiter             []byte      // 分隔符
	valueMaxScanSizeLimit int         // value 最大扫描长度限制
	valueFilter           ValueFilter // value过滤器

	started int32 // 是否已启动
	stopped int32 // 是否已停止
}

func NewSplitter(conf Conf) Splitter {
	if len(conf.Delim) == 0 {
		panic("delim must not be empty")
	}
	s := &splitter{
		chunkSizeLimit:    max(conf.ChunkSizeLimit, MinChunkSizeLimit),
		chunkBuffer:       bytes.NewBuffer(make([]byte, 0, conf.ChunkSizeLimit)),
		chunkSn:           0,
		chunkStartValueSn: 0,
		nextValueSn:       0,
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

// 运行分隔
func (s *splitter) RunSplit(rd io.Reader) error {
	// 防止重复调用
	if atomic.AddInt32(&s.started, 1) != 1 {
		return ErrSplitterIsStarted
	}

	// 创建值读取器
	vr := NewValueReader(rd, s.delimiter, s.valueMaxScanSizeLimit)

	for {
		if atomic.LoadInt32(&s.stopped) > 0 {
			return ErrSplitterIsStopped
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
					ChunkSn:      chunkSn,
					StartValueSn: s.chunkStartValueSn,
					EndValueSn:   s.nextValueSn - 1,
					ChunkData:    s.chunkBuffer.Bytes(),
					ScanByteNum:  vr.GetScanByteNum(),
				})
				s.chunkBuffer.Reset()
				s.chunkStartValueSn = s.nextValueSn
			}

			s.chunkBuffer.Write(value)
			s.chunkBuffer.Write(s.delimiter) // 写入值后要写入分隔符
			s.nextValueSn++
		}

		// 在 EOF 时处理最后一个 chunk
		if err == io.EOF {
			if s.chunkBuffer.Len() > 0 {
				chunkSn := s.chunkSn
				s.chunkSn++
				s.flushChunk(&FlushChunkArgs{
					ChunkSn:      chunkSn,
					StartValueSn: s.chunkStartValueSn,
					EndValueSn:   s.nextValueSn - 1,
					ChunkData:    s.chunkBuffer.Bytes(),
					ScanByteNum:  vr.GetScanByteNum(),
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
	src := args.ChunkData[:len(args.ChunkData)-len(s.delimiter)]

	// 创建副本
	bs := make([]byte, len(src))
	copy(bs, src)

	args.ChunkData = bs
	s.flushChunkHandler(args)
}

func (s *splitter) Stop() {
	atomic.AddInt32(&s.stopped, 1)
}

func defaultFlushChunkHandler(args *FlushChunkArgs) {
	fmt.Println(args.ChunkSn, args.StartValueSn, args.EndValueSn, string(args.ChunkData))
}
