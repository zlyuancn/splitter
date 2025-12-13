package main

import (
	"strings"

	"github.com/zlyuancn/splitter"
)

func main() {
	input := strings.NewReader("apple,banana,pear,peach,cherry")

	conf := splitter.Conf{
		Delim:          []byte(","),
		ChunkSizeLimit: 16,
		FlushChunkHandler: func(args *splitter.FlushChunkArgs) {
			println("Chunk", args.ChunkSn, "values", args.StartValueSn, "to", args.EndValueSn, ":", string(args.ChunkData))
		},
		ValueFilter: func(v []byte) []byte {
			if string(v) == "banana" {
				return nil // 丢弃 banana
			}
			return v
		},
	}

	s := splitter.NewSplitter(conf)
	err := s.RunSplit(input)
	if err != nil {
		panic(err)
	}
}
