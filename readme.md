# `splitter` 模块文档

## 概述

`splitter` 是一个用于将输入流（`io.Reader`）按指定分隔符切分为多个“值”（value），并进一步将这些值聚合为固定大小的“块”（chunk）进行处理的模块。该模块支持：

- 自定义分隔符
- 块大小限制（`ChunkSizeLimit`）
- 单个值最大扫描长度限制（防止无限读取）
- 值过滤（可丢弃或修改特定值）
- 异步安全的停止机制（`Stop()`）

适用于日志解析、流式数据分片、批量处理等场景。

---

## 核心行为说明

### 分片逻辑

1. **读取 value**  
   使用内部 `ValueReader` 从 `io.Reader` 中按 `Delim` 切分出一个个 value。
    - 若连续读取超过 `ValueMaxScanSizeLimit` 字节仍未找到分隔符，返回错误。

2. **应用过滤器**  
   对每个 value 调用 `ValueFilter`，决定是否保留。

3. **构建 chunk**
    - 将保留的 value（附带分隔符）写入内部缓冲区。
    - 当加入新 value 会导致缓冲区总长度 > `ChunkSizeLimit` 时：
        - 触发 `FlushChunkHandler`
        - 清空缓冲区，重置起始索引
    - **例外**：若单个 value 本身已超过 `ChunkSizeLimit`，仍会作为一个独立 chunk 输出（此时 chunk 长度 > 限制）。

4. **结束处理**  
   遇到 `io.EOF` 时，flush 剩余缓冲区内容（即使未满）。

### 停止机制

- 调用 `Stop()` 后，将在**当前 value 处理完毕后**退出循环。
- 无法中断 `ValueReader` 正在进行的扫描（这是设计权衡，避免复杂状态管理）。

### 默认行为

- 若未提供 `FlushChunkHandler`，将使用 `defaultFlushChunkHandler`，即打印到标准输出：
  ```go
  fmt.Println(args.ChunkSn, args.StartValueSn, args.EndValueSn, string(args.ChunkData))
  ```

---

## 使用示例

[传送门](./example/)

```go
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
```

**输出：**
```
Chunk 0 values 0 to 2 : apple,pear,peach
Chunk 1 values 3 to 3 : cherry
```

---

## 接口与类型

### `Splitter` 接口

```go
type Splitter interface {
    // 从 io.Reader 中读取数据，按配置进行分片和处理。
    // 仅允许调用一次，重复调用将返回错误。
	RunSplit(rd io.Reader) error

    // Stop 请求停止处理。注意：无法中断当前正在读取的 value，
    // 但会在完成当前 value 后退出。
    Stop()
}
```

### 配置结构体 `Conf`

```go
type Conf struct {
    Delim                 []byte            // 必填：用于分隔 value 的字节序列（如 "\n"、"\r\n" 等）
    ChunkSizeLimit        int               // 块大小上限（字节数）。默认最小为 16
    FlushChunkHandler     FlushChunkHandler // 块处理回调函数（必提供或使用默认）
    ValueMaxScanSizeLimit int               // 单个 value 最大扫描长度（防 DoS），默认最小为 4096
    ValueFilter           ValueFilter       // 可选：对每个 value 进行过滤或转换
}
```

### 回调函数类型

#### `FlushChunkHandler`

```go
type FlushChunkArgs struct {
    ChunkSn      int    // chunk sn
    StartValueSn int64  // 第一个 value 的 sn
    EndValueSn   int64  // 最后一个 value 的 sn
    ChunkData    []byte // chunk数据
    ScanByteNum  int64  // 已扫描rd的字节数
}

// flush Chunk 函数
type FlushChunkHandler func(args *FlushChunkArgs)
```

- `ChunkSn`：块序号（默认从 0 开始递增）
- `StartValueSn`：该块中第一个 value 的全局索引（从 0 开始）
- `EndValueSn`：该块中最后一个 value 的全局索引
- `ChunkData`：该块的原始字节数据（**不包含末尾分隔符**）
- `ScanByteNum` 传入的 rd(io.Reader) 被扫描了多少字节

⚠️ 注意：`data` 是内部缓冲区的**副本**，可安全持有或修改。

#### `ValueFilter`

```go
type ValueFilter func(value []byte) []byte
```

- 输入：原始 value（不含分隔符）
- 返回：
    - 若返回非空字节切片，则保留该 value
    - 若返回 `nil` 或空切片 `[]byte{}`，则丢弃该 value

---

## 常量

| 常量 | 值 | 说明 |
|------|----|------|
| `MinChunkSizeLimit` | 16 | `ChunkSizeLimit` 的最小允许值 |
| `MinValueMaxScanSizeLimit` | 4096 | `ValueMaxScanSizeLimit` 的最小允许值 |

若配置值低于上述常量，将自动提升至最小值。

---

## 错误处理

- `Delim` 为空 → `panic`
- 重复调用 `RunSplit()` → 返回 `"splitter is started"` 错误
- 单个 value 扫描超长 → 返回 `"ValueReader valueMaxScanSizeLimit err"` 错误
- 其他 I/O 错误 → 直接透传

---

## 注意事项

- **线程安全**：`Splitter` 实例**是线程安全的**，但是不应在多个 goroutine 中并发调用 `RunSplit()`，因为它只能调用一次。
- **内存拷贝**：每次 flush 时会对 chunk 数据做完整拷贝，确保回调函数可安全持有数据。
- **分隔符处理**：chunk 的 `data` **不包含末尾分隔符**，但内部如果有多个 `value` 则每个 `value` 直接会有分隔符。
