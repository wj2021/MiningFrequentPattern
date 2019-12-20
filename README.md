# MapReduce实验5： 频繁项集挖掘
实验要求见实验5频繁项集挖掘.pdf

input为输入数据目录

## Hadoop实现
src/main/java/ex5为Hadoop实现版本，实验可参考论文 PSON: A Parallelized SON Algorithm with MapReduce for Mining Frequent Sets

命令行参数：\<input> \<outputCandidate> \<output> \<support>，分别为输入文件，第一个job的输出目录，第二个job的输出目录，最小支持度

## Spark实现
src/main/scala/ex5为Spark实现版本

