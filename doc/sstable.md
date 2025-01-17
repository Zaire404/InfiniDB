## BloomFilter
用[]byte，hash次数放最后方便直接写到硬盘

### Hash函数
Hash函数选用murmurHash
原因
- 快 比MD5快
- 高混淆 散列均匀，更适合BloomFilter
### 优化
论文Less Hashing, Same Performance: Building a Better Bloom Filter提到可以采用两次hash的方式来替代传统多次hash运算