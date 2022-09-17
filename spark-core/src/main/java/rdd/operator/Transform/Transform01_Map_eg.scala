package rdd.operator.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Transform01_Map_eg {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDOperator")
    val sc = new SparkContext(sparkConf)

    // Example: 从服务器日志数据 apache.log 获取用户请求URL资源路径
    val rdd: RDD[String] = sc.textFile("data/apache.log")
    val mapRDD: RDD[String] = rdd.map(
      line => {
        val datas = line.split(" ")
        datas(6)
      }
    )
    mapRDD.collect().foreach(println)

    sc.stop()
  }
}
