package ex5

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.immutable.TreeSet
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
 * 频繁项集挖掘
 * 抛弃局部计算频繁项集的做法 => 并行化统计每个候选项集出现的次数
 * 使用TreeSet表示每条Transaction
 */
object MiningFrequentPatternSpark_v1 {
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

    // 总transaction大小 和 频繁项需要达到的最小出现次数
    val totalTransactionCount: Long = textRDD.count()
    val minCount: Int = Math.ceil(totalTransactionCount * minSupp).toInt

    // transaction集合，每个transaction中的Items表示为一个集合（Set），预处理源数据，将相同的transaction计数
    val transactions: RDD[(TreeSet[Item],Int)] = textRDD.map(line=>{
      var transaction: TreeSet[Item] = TreeSet()
      line.split("\\s+").foreach(transaction += new Item(_))
      (transaction, 1)
    }).reduceByKey(_+_).cache()

    //计算1-频繁项集
    var K: Int = 1 // 当前计算K频繁项集，即频繁项中的元素有K个
    val frequentPattern: RDD[(TreeSet[Item], Int)] = textRDD.flatMap(_.split("\\s+")).map(word=>(TreeSet(new Item(word)),1)).reduceByKey(_+_).filter(_._2 >= minCount).cache()
    val result1: RDD[(TreeSet[Item], Double)] = frequentPattern.map(x=>(x._1, x._2 / totalTransactionCount.toDouble))
    result1.saveAsTextFile(args(1) + java.io.File.separator + K)

    var pattern_sets: Array[TreeSet[Item]] = result1.map(_._1).collect()
    while(pattern_sets.nonEmpty) {
      K += 1
      // 利用K-频繁项集生成(K+1)-候选频繁项集
      var candidate_pattern_sets: Set[TreeSet[Item]] = Set()
      for (i <- pattern_sets.indices; j <- i + 1 until pattern_sets.length) {
        if (K == 2) {
          val z: TreeSet[Item] = pattern_sets(i) ++ pattern_sets(j)
          candidate_pattern_sets += z
        } else {
          val len = pattern_sets(i).size
          if(pattern_sets(i).slice(0, len-1).equals(pattern_sets(j).slice(0, len-1))) {
            val z = pattern_sets(i) + pattern_sets(j).lastKey
            // 过滤存在不频繁子集的候选集合
            var hasInfrequentSet = false
            breakable {
              z.subsets(pattern_sets(i).size).foreach(set=>{
                if (!pattern_sets.contains(set)) {
                  hasInfrequentSet = true
                  break
                }
              })
            }
            if(!hasInfrequentSet) candidate_pattern_sets += z
          }
        }
      }

      // 根据候选频繁项集获得频繁项集
      val bcCFI: Broadcast[Set[TreeSet[Item]]] = sc.broadcast(candidate_pattern_sets)
      val nextFrequentPattern = transactions.flatMap(line=>{
        var tmp: ArrayBuffer[(TreeSet[Item], Int)] = ArrayBuffer()
        bcCFI.value.foreach(itemSet=>{
          if(itemSet.subsetOf(line._1)) {
            tmp :+= Tuple2(itemSet, line._2)
          }
        })
        tmp
      }).reduceByKey(_+_).filter(_._2 >= minCount).cache()
      if(nextFrequentPattern.count > 0){
        val resultk = nextFrequentPattern.map(x=>(x._1, x._2 / totalTransactionCount.toDouble))
        resultk.saveAsTextFile(args(1) + java.io.File.separator + K)
      }
      bcCFI.unpersist()
      pattern_sets = nextFrequentPattern.map(_._1).collect()
    }

  }

}

class Item(val value: String) extends Comparable[Item] with Serializable {
  override def compareTo(that: Item): Int = {
    val lena = this.value.length
    val lenb = that.value.length
    if(lena == lenb) {
      this.value.compareTo(that.value)
    } else {
      lena - lenb
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case item: Item =>
        this.value.equals(item.value)
      case _ =>
        false
    }
  }

  override def toString: String = value

  override def hashCode(): Int = value.hashCode
}