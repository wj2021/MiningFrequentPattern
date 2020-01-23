package ex5

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
 * 使用BitSet来表示每条transaction，优化速度
 */
object MiningFrequentPatternSpark_v2 {
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
    val transactions: RDD[(java.util.BitSet,Int)] = textRDD.map((_,1)).reduceByKey(_+_).map(x=>{
      val transaction: java.util.BitSet = new java.util.BitSet()
      x._1.split("\\s+").foreach(y=>transaction.set(y.toInt, true))
      (transaction, x._2)
    }).cache()

    //计算1-频繁项集
    var K: Int = 1 // 当前计算K频繁项集，即频繁项中的元素有K个
    val frequentPattern: RDD[(String, Int)] = transactions.flatMap(transaction=>{
      val tmp = new ArrayBuffer[(String, Int)]
      for (i <- 0 until transaction._1.size()) {
        if (transaction._1.get(i)) tmp += Tuple2(i.toString, transaction._2)
      }
      tmp
    }).reduceByKey(_+_).filter(_._2 >= minCount).cache()
    val result1: RDD[(String, Double)] = frequentPattern.map(x=>(x._1, x._2 / totalTransactionCount.toDouble))
    result1.saveAsTextFile(args(1) + java.io.File.separator + K)

    var frequentItemSets: Array[String] = frequentPattern.map(_._1).collect()
    while(frequentItemSets.nonEmpty) {
      K += 1
      val candidateFrequentItemSets: Array[String] = getCandidateFrequentItemSets(frequentItemSets, K)
      // 根据候选频繁项集获得频繁项集
      val bcCFI: Broadcast[Array[String]] = sc.broadcast(candidateFrequentItemSets)
      val nextFrequentPattern: RDD[(String, Int)] = transactions.flatMap(transaction=>{
        val tmp: ArrayBuffer[(String, Int)] = ArrayBuffer()
        bcCFI.value.foreach(itemSet=>{
          var exist = true
          val itemArray: Array[String] = itemSet.split(",")
          breakable {
            for(i <- itemArray) {
              if(!transaction._1.get(i.toInt)) {
                exist = false
                break
              }
            }
          } // breakable
          if(exist) tmp += Tuple2(itemSet, transaction._2)
        })
        tmp
      }).reduceByKey(_+_).filter(_._2 >= minCount).cache()
      val resultk: RDD[(String, Double)] = nextFrequentPattern.map(x=>(x._1, x._2 / totalTransactionCount.toDouble))
      if(resultk.count > 0){
        resultk.saveAsTextFile(args(1) + java.io.File.separator + K)
      }
      bcCFI.unpersist()
      frequentItemSets = resultk.map(_._1).collect()
    }

  }

  // 利用K-1频繁项集生成K-候选频繁项集
  // patternSet: K-1频繁项集
  // k：需要生成的候选频繁项集的项数
  def getCandidateFrequentItemSets(lastFrequentItemSets: Array[String], k: Int): Array[String] = {
    var candidateFrequentItemSets: ArrayBuffer[String] = ArrayBuffer()
    for(i <- lastFrequentItemSets.indices; j <- i + 1 until lastFrequentItemSets.length) {
      var candidateFrequentItemSet = ""
      if(k == 2) {
        candidateFrequentItemSet = (lastFrequentItemSets(i)+","+lastFrequentItemSets(j)).split(",").sortWith((a, b)=>a.toInt<=b.toInt).reduce(_+","+_)
      }
      else if(lastFrequentItemSets(i).substring(0,lastFrequentItemSets(i).lastIndexOf(",")).equals(lastFrequentItemSets(j).substring(0,lastFrequentItemSets(j).lastIndexOf(",")))) {
        candidateFrequentItemSet = (lastFrequentItemSets(i)+lastFrequentItemSets(j).substring(lastFrequentItemSets(j).lastIndexOf(","))).split(",").sortWith((a, b)=>a.toInt<=b.toInt).reduce(_+","+_)
      }
      //过滤存在不频繁子集的候选集合
      var hasInfrequentSubItem = false
      if(!"".equals(candidateFrequentItemSet) && k != 2) {
        val itemSet = candidateFrequentItemSet.split(",")
        breakable {
          for (i <- itemSet.indices) {
            var subSet = ""
            for (j <- itemSet.indices) {
              if (i != j) {
                subSet += itemSet(j) + ","
              }
            }
            subSet = subSet.substring(0, subSet.lastIndexOf(","))
            if (!lastFrequentItemSets.contains(subSet)) {
              hasInfrequentSubItem = true
              break
            }
          }
        } // breakable
      } else if(k != 2) {
        hasInfrequentSubItem = true
      }
      if(!hasInfrequentSubItem) {
        candidateFrequentItemSets +:= candidateFrequentItemSet
      }
    }
    candidateFrequentItemSets.toArray
  }

}
