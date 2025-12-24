package splitter

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"math"

	"golang.org/x/time/rate"
)

var ErrValueReaderMaxScanSizeLimit = errors.New("ValueReader valueMaxScanSizeLimit err")

type ValueReader interface {
	// 下一个value
	Next() ([]byte, error)
	// 获取已扫描字节数
	GetScanByteNum() int64
}

type valueReader struct {
	reader *bufio.Reader

	readBuffer []byte

	delim                 []byte
	valueMaxScanSizeLimit int // 限制value的长度

	scanByteNum int64 // 已扫描字节数
	isEOF       bool

	limiter *rate.Limiter // 限速器
}

func (v *valueReader) GetScanByteNum() int64 {
	return v.scanByteNum
}

// 读取数据直到碰到一个分隔符, 输出数据不包含分隔符. 注意使用者要主动对返回的[]byte进行copy, 否则下次调用此函数会改变它!
func (v *valueReader) Next() ([]byte, error) {
	if v.isEOF {
		return nil, io.EOF
	}

	l := 0

	delimLen := len(v.delim)
	last := v.delim[delimLen-1]

	for {
		// 限速
		if v.limiter != nil {
			err := v.limiter.Wait(context.Background())
			if err != nil {
				return nil, err
			}
		}

		// 读取1字节
		b, err := v.reader.ReadByte()
		if err == io.EOF {
			v.isEOF = true
			return v.readBuffer[:l], nil
		}
		if err != nil {
			return nil, err
		}

		v.scanByteNum++
		v.readBuffer[l] = b
		l++
		bs := v.readBuffer[:l]

		// 检查是否以 delim 结尾
		if b == last && l >= delimLen && bytes.Equal(bs[l-delimLen:], v.delim) {
			return bs[:l-delimLen], nil
		}

		// 检查长度限制
		if l == v.valueMaxScanSizeLimit {
			return bs, ErrValueReaderMaxScanSizeLimit
		}
	}
}

// 创建一个值读取器
func NewValueReader(rd io.Reader, delim []byte, valueMaxScanSizeLimit int) ValueReader {
	return NewValueReaderAndLimiter(rd, delim, valueMaxScanSizeLimit, 0)
}

// 创建一个值读取器, 限制其读取速率
func NewValueReaderAndLimiter(rd io.Reader, delim []byte, valueMaxScanSizeLimit int, rateLimit int) ValueReader {
	if len(delim) == 0 {
		panic("delim must not be empty")
	}

	bufLen := max(valueMaxScanSizeLimit, MinValueMaxScanSizeLimit)
	vr := &valueReader{
		reader:                bufio.NewReader(rd),
		readBuffer:            make([]byte, bufLen),
		delim:                 delim,
		valueMaxScanSizeLimit: bufLen,
	}
	if rateLimit > 0 {
		bursts := math.Max(float64(int(rateLimit)/10), 1) // 爆发量为上限的十分之一
		vr.limiter = rate.NewLimiter(rate.Limit(rateLimit), int(bursts))
	}
	return vr
}
