package rdd.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object partitioner01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("partitioner")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("nba", "x"),
      ("wnba", "xx"),
      ("cba", "xxx"),
      ("nba", "xxxx")
    ), 3)

    val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)

    partRDD.saveAsTextFile("output")

    sc.stop()
  }

  /*
  自定义分区，按照自己的规则进行数据分区
  1. 继承partitioner
  2. 重写方法
  */
  class MyPartitioner extends Partitioner {
    // 分区数量
    override def numPartitions : Int = 3

    // 根据数据的 key 返回数据所在的分区索引（从0开始）
    override def getPartition(key : Any) : Int = {
      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }
    }
  }
}
