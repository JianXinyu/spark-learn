package practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Req1_HotCategoryTop10_Improve2 {

  def main(args : Array[String]) : Unit = {
    // TODO: Top10热门品类数量
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1_HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    // Problem:
    // 为什么不一开始就把数据源变为（品类ID，(点击数, 0, 0)) 呢？
    // 这样就能减少 reduceByKey，因为只要有 reduceByKey 就有 shuffle 操作
    // 目前存在大量 shuffle 操作
    
    // 1. import data
    val actionRDD = sc.textFile("data/user_visit_action.txt")
    actionRDD.cache()

    // 2. 将数据转换结构
    // （品类ID，(1, 0, 0)), （品类ID，(0, 1, 0)), （品类ID，(0, 0, 1))
    val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          // 点击
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          // 下单
          val ids = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          // 支付
          val ids = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    // 3. 将相同品类ID的数据进行分组聚合
    // （品类ID，(点击数, 下单数, 支付数))
    val analysisRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    // 4. 排序取top 10
    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)

    // 6. 输出结果
    resultRDD.foreach(println)

    sc.stop()
  }

}
