## 题目

某个机器的配置为：CPU 8 cores, MEM 4G, HDD 4T
这个机器上有一个 1T 的无序数据文件，格式为 (key_size: uint64, key: bytes, value_size: uint64, value: bytes)，其中 1B <= key_size <= 1KB，1B <= value_size <= 1MB。

**作业要求**：

设计一个索引结构，使得并发随机地读取每一个 key-value 的代价最小
允许对数据文件做任意预处理，但是预处理的时间计入到整个读取过程的代价里