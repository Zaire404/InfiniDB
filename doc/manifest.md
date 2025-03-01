# manifest

```go
<beginning_of_file>
[magic number]
[commit 1]
[commit 2]
...
[commit N]
<end_of_file>
```
`change`和`commit`定义在proto/codec.proto

其中：
- commit：一个数据结构，表示一次提交记录，包含提交的长度、校验码以及多个变更项（change）。
- change：一个数据结构，描述单一的变更记录，反映了在一次提交中发生的具体修改。

详情请参阅 [proto/codec.proto](proto/codec.proto) 文件中的定义。
```go
// 详见[file/manifest]中addChanges()函数
[commit]
	- [length]		// length of the changes data
	- [checksum]	// checksum of the changes data
	- [change 1]
	- [change 2]
	...
	- [change N]
```