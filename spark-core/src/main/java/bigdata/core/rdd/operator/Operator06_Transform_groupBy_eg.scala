package bigdata.core.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Operator06_Transform_groupBy_eg {
  def main(args : Array[String]) : Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator-groupBy")
    val sc = new SparkContext(sparkConf)

    // 从服务器日志数据 apache.log 中获取每个时间段访问量

    val rdd: RDD[String] = sc.textFile("data/apache.log")


    val groupRDD: RDD[(Any, Iterable[String])] = rdd.groupBy(_.charAt(0))
    groupRDD.collect().foreach(println)
  }
}
