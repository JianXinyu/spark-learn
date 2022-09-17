package rdd.operator.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

object Transform06_groupBy_eg {
  def main(args : Array[String]) : Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator-groupBy")
    val sc = new SparkContext(sparkConf)

    // Example: 从服务器日志数据 apache.log 中获取每个时间段访问量

    val rdd: RDD[String] = sc.textFile("data/apache.log")

    // 分解，取出小时段,
    val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(
      lines => {
        val datas = lines.split(" ")
        val time = datas(3)
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date: Date = sdf.parse(time)
        val sdf1 = new SimpleDateFormat("HH")
        val hours: String = sdf1.format(date)
        (hours, 1)
      }
    ).groupBy(_._1)

    timeRDD.map {
      case (hour, iter) =>
        (hour, iter.size)
    }.collect.foreach(println)
  }
}
