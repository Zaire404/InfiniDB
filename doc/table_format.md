```go
<beginning_of_file>
[data block 1]
[data block 2]
...
[data block N]
[index block]
	- [indexData]
	- [indexLen]
	- [indexChecksum]
	- [indexChecksumLen]
<end_of_file>
```
`data block`是lsm/builder.go中的block
```go
[data block]
	- [KV 1]
		- [header]
			- [overlap]
			- [diff]
		- [diffKey]
		- [value]
	- [KV 2]
	...
	- [KV n]
	- [entryOffsets]
	- [entryOffsetsLen]
	- [Checksum]
	- [ChecksumLen]
```