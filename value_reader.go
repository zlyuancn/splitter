package splitter

import (
	"bufio"
	"bytes"
	"errors"
	"io"
)

var ErrValueReaderMaxScanSizeLimit = errors.New("ValueReader valueMaxScanSizeLimit err")

type ValueReader interface {
	Next() ([]byte, error)
}

type valueReader struct {
	reader *bufio.Reader

	readBuffer []byte

	delim                 []byte
	valueMaxScanSizeLimit int // 限制value的长度

	isEOF bool
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
		b, err := v.reader.ReadByte()
		if err == io.EOF {
			v.isEOF = true
			return v.readBuffer[:l], nil
		}
		if err != nil {
			return nil, err
		}

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

func NewValueReader(rd io.Reader, delim []byte, valueMaxScanSizeLimit int) ValueReader {
	if len(delim) == 0 {
		panic("delim must not be empty")
	}

	bufLen := max(valueMaxScanSizeLimit, MinValueMaxScanSizeLimit)
	return &valueReader{
		reader:                bufio.NewReader(rd),
		readBuffer:            make([]byte, bufLen),
		delim:                 delim,
		valueMaxScanSizeLimit: bufLen,
	}
}
