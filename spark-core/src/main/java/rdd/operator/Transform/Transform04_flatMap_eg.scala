package rdd.operator.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Transform04_flatMap_eg {
  def main(args: Array[String]) : Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator-flatMap")
    val sc = new SparkContext(sparkConf)
   /*
   flatMap: 扁平映射，将处理的数据进行扁平化后再进行映射处理
   */

    // 将 List(List(1,2),3,List(4,5))进行扁平化操作
    val rdd = sc.makeRDD(List(
      List(1,2),3,List(4,5)
    ))
    // 模式匹配
    val flatRdd = rdd.flatMap {
      case list: List[_] => list
      case dat => List(dat)
    }
    flatRdd.collect().foreach(println)
    sc.stop()
  }
}
