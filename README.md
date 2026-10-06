# singleflightlite

一个零依赖、并发安全的 Go singleflight 库：相同 key 的并发调用合并为一次执行，不同 key 并行。仅提供内存中的去重语义，不含缓存、TTL、重试、网络或持久化。

## 语义

- 相同 key 的并发 `Do` 只有一个 leader 真正执行 `fn`（在全局锁之外执行），其余 follower 等待并共享其结果。
- `Do` 返回 `(value, shared, err)`；`shared=true` 表示本次调用复用了进行中的 flight 结果，而非自己执行 `fn`。
- follower 的 ctx 取消只让自己提前返回 `ctx.Err()`，不影响 leader 和其他等待者；leader 的 ctx 会传入 `fn`，是否响应取消由 `fn` 决定。
- flight 完成后立即从 Group 移除，不做缓存；`Forget(key)` 可让同 key 的下一次 `Do` 立刻开启新 generation，旧 flight 照常完成且不会误删新 flight。

## API

```go
g := singleflightlite.NewGroup() // 零值 Group 也可直接使用

val, shared, err := g.Do(ctx, "key", func(ctx context.Context) (any, error) {
    return loadExpensive(ctx) // 同一时刻对同一 key 只执行一次
})

g.Forget("key")      // 丢弃进行中的 flight，下次 Do 开新 generation
infos := g.Snapshot() // 当前可加入的 flight（Key/Generation/Waiters），按 key 排序
n := g.Len()          // 进行中 flight 数量
```

参数非法时返回哨兵错误：`ErrNilContext`、`ErrEmptyKey`、`ErrNilFunc`。

## 运行演示 CLI

```sh
go run ./cmd/sfdemo
```

依次演示：同 key 合并（4 个调用只执行 1 次）、follower 取消（leader 不受影响）、`Forget` 开启新 generation。

## 测试

```sh
go test -race ./...
```

测试用 channel 控制执行顺序（无 sleep），覆盖共享执行、不同 key 并行、error 共享、follower 取消、Forget generation 隔离、非法参数、Snapshot/Len 与并发压力。

