# MapReduce实验5： 频繁项集挖掘
实验要求见实验5频繁项集挖掘.pdf

input为输入数据目录

## Hadoop实现
src/main/java/ex5为Hadoop实现版本，实验可参考论文 PSON: A Parallelized SON Algorithm with MapReduce for Mining Frequent Sets

命令行参数：\<input> \<outputCandidate> \<output> \<support>，分别为输入文件，第一个job的输出目录，第二个job的输出目录，最小支持度

如果使用input_chess目录作为输入任务会失败，因为第一个Mapper中的Apriori算法太耗时导致系统超时。将chess.dat文件分片后局部频繁项反而算不出来，说明Apriori算法性能与数据相关度很高，当数据中每一个transaction的相似度很高时(如input_chess/test.dat，该文件的每个transaction拥有相同的items)，算法无法有效剪枝，会出现组合爆炸。-----------------数据分片有时并不能加快计算速度，还要考虑如何将数据分片使同一分片内的数据相似性尽可能低。

## Spark实现
src/main/scala/ex5为Spark实现版本

