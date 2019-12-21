package ex5

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import collection.JavaConverters._

/**
 * 频繁项集挖掘
 * 基于Apriori算法，使用spark并行化
 */
object MiningFrequentPatternSpark {
  def main(args: Array[String]): Unit = {
    // 参数验证
    if (args.length != 3) {
      throw new Exception("The input arguments must be <input> <output> <support>")
    }

    // 从命令行参数读取最小支持度，用来过滤不频繁的项集
    val minSupp = args(2).toDouble
    if (minSupp < 0 || minSupp > 1) {
      throw new Exception("input support must between 0 and 1")
    }

    // 删除输出目录
    val path = new Path(args(1))
    val hdfs = FileSystem.get(new Configuration())
    if(hdfs.exists(path)) {
      hdfs.delete(path, true)
    }

    // Spark配置
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Apriori FPM")

    val sc = new SparkContext(conf)

    // 读取输入文件，文件路径为第一个命令行参数
    val textRDD: RDD[String] = sc.textFile(args(0))

    // 定义 总transaction大小 和 频繁项需要达到的最小出现次数
    val totalTransactionCount: Long = textRDD.count()
    val minCount: Int = Math.ceil(totalTransactionCount * minSupp).toInt

    // 在每个分区中利用apriori算法计算频繁项集，汇总作为候选频繁项集
    val localFrequentPattern: RDD[scala.collection.mutable.Set[Int]] = textRDD.mapPartitionsWithIndex((partId, Iterator) => {
      val part_map = scala.collection.mutable.Map[Int, List[String]]()
      part_map(partId) = List[String]()
      while (Iterator.hasNext) {
        part_map(partId) :+= Iterator.next()
      }
      val result: List[scala.collection.mutable.Set[Int]] = apriori(part_map(partId), minSupp)
      result.iterator
    }).map((_, 1)).reduceByKey(_ + _).map(_._1)

    localFrequentPattern.cache()

    // 从候选频繁项集中得到全局频繁项集
    val candidateFrequent: Array[scala.collection.mutable.Set[Int]] = localFrequentPattern.collect()
    textRDD.flatMap(line => {
      val transaction: scala.collection.mutable.Set[Int] = scala.collection.mutable.TreeSet()
      line.split("\\s+").map(_.toInt).foreach(transaction.add);
      val itr = candidateFrequent.iterator
      var result = Array[(scala.collection.mutable.Set[Int], Int)]()
      while (itr.hasNext) {
        val pattern = itr.next()
        var exist = true
        val itr2 = pattern.iterator
        while (itr2.hasNext) {
          if (!transaction.contains(itr2.next())) {
            exist = false
          }
        }
        if (exist) {
          result :+= (pattern, 1)
        }
      }
      result
    }).reduceByKey(_+_).filter(_._2 >= minCount).map(x=>(new ItemSet(x._1.toString).sortSet, x._2/totalTransactionCount.toDouble)).sortByKey().saveAsTextFile(args(1))

  }


  // 使用apriori算法计算频繁项集
  def apriori(transactions: List[String], minSup: Double): List[scala.collection.mutable.Set[Int]] = {
    val transactionList: util.List[Transaction[String]] = new util.ArrayList()
    for(str <- transactions) {
      val transaction: Transaction[String] = new Transaction(str, "\\s+")
      transactionList.add(transaction)
    }
    val apriori: Apriori[String] = new Apriori(transactionList, minSup)
    val result1: util.Map[util.Set[String], Integer] = apriori.getFrequentPattern
    val result2: scala.collection.mutable.Map[util.Set[String], Integer] = result1.asScala
    var result: List[scala.collection.mutable.Set[Int]] = List()
    for(r <- result2) {
      result :+= r._1.asScala.map(_.toInt)
    }
    result
  }

  // 自定义项集的排序
  class ItemSet(val sets: String) extends Ordered[ItemSet] with Serializable {
    override def compare(that: ItemSet): Int = {
      val tokens1:Array[String] = this.sets.substring(this.sets.indexOf("(")+1, this.sets.indexOf(")")).split(",\\s+")
      val tokens2:Array[String] = that.sets.substring(that.sets.indexOf("(")+1, that.sets.indexOf(")")).split(",\\s+")
      var result = 0
      if(tokens1.length != tokens2.length) {
        result = tokens1.length - tokens2.length
      } else {
        var index:Int = 0
        val length:Int = tokens1.length
        while(index < length) {
          if(!tokens1.equals(tokens2)) {
            result = ItemSetText.compareString(tokens1(index), tokens2(index))
            index = length
          }
          index += 1
        }
      }
      result
    }

    // 将项集内部排个序
    def sortSet: ItemSet = {
      val tokens: Array[String] = this.sets.substring(this.sets.indexOf("(")+1, this.sets.indexOf(")")).split(",\\s+")
      implicit val ordering = new Ordering[String] {
        override def compare(x: String, y: String): Int = {
          ItemSetText.compareString(x, y)
        }
      }
      val sortedTokens = tokens.sorted(ordering)
      val ans: StringBuilder = new StringBuilder("Set(")
      for(t <- sortedTokens) {
        ans.append(t).append(", ")
      }
      if(ans.contains(',')) {
        ans.delete(ans.length-2, ans.length)
      }
      ans.append(")")
      new ItemSet(ans.toString())
    }

    override def toString: String = this.sets

  }

}

