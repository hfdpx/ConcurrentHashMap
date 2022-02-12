# concurrentHashMap

采用分段锁机制实现的Golang版本的并发安全Map

* Go 1.9之前，官方并没有实现并发安全的Map.
* Go 1.9中，官方引入了sync.map，并发安全.
* sync.map仅适用于读多写少的情况.在写多读少情况下，sync.map性能并不理想.

**concurrentHashMap**专为写多读少情况而设计，写入一千万数据，耗时不到sync.map的一半。



# 使用

```go
package main

import (
	"fmt"
	"github.com/hfdpx/concurrent-hash-map"
)

func main(){
	// 初始化
	cmap:=concurrentHashMap.New()

	// set
	cmap.Set("key","value")

	// get
	if tmp, ok := cmap.Get("key"); ok {
		value:= tmp.(string)
		fmt.Println(value)
	}

	// delete
	cmap.Remove("key")
}
```
更多样例，请参考_test文件。

> 关于设计思想和性能对比分析，请参考作者博客：https://www.cnblogs.com/yinbiao/p/15884420.html