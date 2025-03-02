## WAL

```go
<beginning_of_file>
[entryCodec1]
[entryCodec2]
...
[entryCodecN]
<end_of_file>
```
详情参阅 [util/wal.go](util/wal.go) 文件中的`WalCodec`。
```go
[entryCodec]
    - [header]
        - [KeyLen]
        - [ValueLen]
        - [Meta]
        - [ExpireAt]
    - [Key]
    - [Value]
    - [Checksum]
```
