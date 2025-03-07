## VLog

```go
<beginning_of_file>
[entryCodec 1]
[entryCodec 2]
...
[entryCodec N]
<end_of_file>
```
header不复用wal的里使用的header,可以减少50%的header的大小
```go 
[entryCodec]
    - [header]
        - [KeyLen]
        - [ValueLen]
    - [Key]
    - [Value]
    - [Checksum]
```

在启动时将所有vlog通过mmap映射到内存，在查询时通过ValuePtr直接获取value的地址，获取实际的Value，减少了一次内存拷贝
