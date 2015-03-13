#定制 ORC 文件格式

##前提
这是一个java项目。
在Eclipse中需要引入Jar包：

 1. hadoop的Jar包
 2. Hive的部分Jar包（hive-exec & hive-metastore 两个jar包）

##说明

###1. 用MR直接读取ORC文件
使用MR直接读取ORC文件，在不修改`OrcNewInputFormat.java`的情况下，Map中接收到的value是`OrcStruct`类型的，具体的OrcStruct结构是由原先写入ORC的时候指定的。

直接在Reduce中保存从ORC读取的文件，默认使用压缩算法，生成`part-r-00000.deflate`格式的压缩文件。可以使用
```bash
hdfs dfs -text file.deflate
```
来查看文件内容