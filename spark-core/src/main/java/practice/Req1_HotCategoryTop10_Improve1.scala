package practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Req1_HotCategoryTop10_Improve1 {

  def main(args : Array[String]) : Unit = {
    // TODO: Top10热门品类数量
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1_HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    // Problem:
    // 1. actionRDD 复用太多 => cache
    // 2. cogroup 可能存在 shuffle，性能受到影响
    // => 数据源变为 （品类ID，(点击数, 0, 0)), （品类ID，(0, 下单数, 0)), （品类ID，(0, 0, 支付数))
    // 这样就能两两聚合

    // 1. import data
    val actionRDD = sc.textFile("data/user_visit_action.txt")
    actionRDD.cache()

    // 2. 统计品类的点击 （品类ID，点击数）
    val clickActionRDD = actionRDD.filter(
      action => {
        val data = action.split("_")
        data(6) != "-1"
      }
    )

    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map(
      action => {
        val data = action.split("_")
        (data(6), 1)
      }
    ).reduceByKey(_+_)

    // 3. 统计品类的下单 （品类ID，下单数）
    val orderActionRDD = actionRDD.filter(
      action => {
        val data = action.split("_")
        data(8) != "null"
      }
    )

    val orderCountRDD: RDD[(String, Int)] = orderActionRDD.flatMap(
      action => {
        val data = action.split("_")
        val cid = data(8) // category id
        val cids = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    // 4. 统计品类的支付 （品类ID，支付数）
    val payActionRDD = actionRDD.filter(
      action => {
        val data = action.split("_")
        data(10) != "null"
      }
    )

    val payCountRDD: RDD[(String, Int)] = payActionRDD.flatMap(
      action => {
        val data = action.split("_")
        val cid = data(10) // category id
        val cids = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    // 5. 将品类排序，取Top 10
    //    点击数量排序 => 下单数量排序 => 支付数量排序
    //    元组排序：先比较第一个，再比较第二个，依次类推
    //    =>（品类ID，（点击数，下单数，支付数） ）
    val rdd1 = clickCountRDD.map{
      case ( cid, cnt ) => {
        (cid, (cnt, 0, 0))
      }
    }
    val rdd2 = orderCountRDD.map{
      case ( cid, cnt ) => {
        (cid, (0, cnt, 0))
      }
    }
    val rdd3 = payCountRDD.map{
      case ( cid, cnt ) => {
        (cid, (0, 0, cnt))
      }
    }

    // 将三个数据源合并在一起，统一进行聚合计算
    val sourceRDD: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)

    val analysisRDD: RDD[(String, (Int, Int, Int))] = sourceRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)

    // 6. 输出结果
    resultRDD.foreach(println)

    sc.stop()
  }

}
