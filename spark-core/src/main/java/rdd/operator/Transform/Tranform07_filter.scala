package rdd.operator.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Tranform07_filter {
  def main(args : Array[String]) : Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator-filter")
    val sc = new SparkContext(sparkConf)
    /*
    filter: 将数据根据指定的规则进行筛选过滤。
    分区不变，但是分区内的数据可能不均衡，生产环境下，可能会出现数据倾斜
     */

//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
//    val filterRDD: RDD[Int] = rdd.filter(num => num % 2 != 0)
//    filterRDD.collect().foreach(println)

    // Example：从服务器日志数据apache.log中获取2015年5月17日的请求路径
    val rdd: RDD[String] = sc.textFile("data/apache.log")
    rdd.filter(
      line => {
        val datas = line.split(" ")
        val time = datas(3)
        time.startsWith("17/05/2015")
      }
    ).collect().foreach(println)

    sc.stop()
  }

}
