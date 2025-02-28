# Compact

lsm建立时开几个线程去Compact

## 策略
[Dynamic Leveled Compaction](https://rocksdb.org/blog/2015/07/23/dynamic-level.html)，自底向上填充，每一层的大小是根据最后一层的大小来动态调整的，防止出现前几层全满，最后一层太小的问题，降低写放大

## 实现

### 选择srcLevel和dstLevel
每次压缩都计算一次各层应该达到的总大小、table大小，L+1层的大小是L层的T倍
一开始所有非零层都为空，从最后一层开始填充，srcLevel=0, dstLevel=MaxLevel



### 合并
#### L0->Ln
直接将L0的各个Table都作为一个输入源，将Ln的与L0有哦overlap的全部Table作为一个输入源(ConcatIterator实现)。
#### Ln->Ln+1
Ln在一个合并任务中只会有一个Table要和Ln+1的一些Table合并。

