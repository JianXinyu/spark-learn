package accumulator

import org.apache.spark.{SparkConf, SparkContext}

object Accumulator01_normal_var {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("accumulator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4,5))

    // reduce 是分区内计算 + 分区间计算
//    val i : Int = rdd.reduce(_+_)
//    println(i)

    var sum = 0
    rdd.foreach(
      num => {
        sum += num
      }
    )
    println(" sum = " + sum)

    sc.stop()
  }
  }
