package bigdata.core.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Operator01_Transform_Map_Parrallelism {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDOperator")
    val sc = new SparkContext(sparkConf)

    // 乱序打印，是并行的体现
    // 1. RDD的计算在一个分区内的数据是顺序执行，只有前一个数据的全部逻辑执行完才会进行下一个数据。
    // 2. 不同分区的数据执行是无序的
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 1)

    val mapRDD = rdd.map(
      num => {
        println(">>>>>>" + num)
        num
      }
    )

    val mapRDD1 = mapRDD.map(
      num => {
        println("######" + num)
        num
      }
    )
    mapRDD1.collect()

    sc.stop()
  }
}
