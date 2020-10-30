### State的数据结构
- ValueState
- ListState
- MapState
- ReducingState

上面讲的state对象仅仅用来与状态进行交互(更新,删除),而真正的状态可以保存在内存,磁盘后者分布式存储系统中.
### StateBackend
默认情况下state是保存在TaskManager的内存中,checkpoint是保存在JobManager的内存中.
State和Checkpoint的存储位置取决与StateBackend的配置
- MemoryStateBackend
- FsStateBackend
- RocksDBStateBackend
### 关于三种存储方式进一步说明
- 1
 
配置
`environment.setStateBackend(new MemoryStateBackend)`
在这种配置下
state是持久在java的堆内存,执行checkpoint会把state的快照数据保存到JobManager的内存中

- 2 

配置
`env.setStateBackend(new FsStateBackend("hdfs://node01:800/flink/checkDir""))`
state是保存在TaskManager的内存中,执行checkpoint时会把state的快照数据保存到配置的文件系统中

- 3

配置
`env.setStateBackend(new RocksDBStatebackend("hdfs://node01:8020/flink/checkDir",true))`
state会存储在本地的RocksDB中,同时会把数据也复制到远端的文件系统中
