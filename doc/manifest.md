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
```go
[commit]
	- [length]
	- [checksum]
	- [change 1]
	- [change 2]
	...
	- [change N]
```