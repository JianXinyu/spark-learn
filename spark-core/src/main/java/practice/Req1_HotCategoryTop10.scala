package practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Req1_HotCategoryTop10 {

  def main(args : Array[String]) : Unit = {
    // TODO: Top10热门品类数量
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1_HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    // 1. import data
    val actionRDD = sc.textFile("data/user_visit_action.txt")
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
    // operator: join, zip, leftOuterJoin, cogroup
    // join不能用，因为要求两个数据源有相同的key
    // TODO zip leftOuterJoin
    // cogroup = connect + group
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
      clickCountRDD.cogroup(orderCountRDD, payCountRDD)
    val analysisRDD: RDD[(String, (Int, Int, Int))] = cogroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {
        var clickCnt = 0
        if (clickIter.iterator.hasNext) {
          clickCnt = clickIter.iterator.next()
        }
        var orderCnt = 0
        if (orderIter.iterator.hasNext) {
          orderCnt = orderIter.iterator.next()
        }
        var payCnt = 0
        if (payIter.iterator.hasNext) {
          payCnt = payIter.iterator.next()
        }

        (clickCnt, orderCnt, payCnt)
      }
    }

    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)

    // 6. 输出结果
    resultRDD.foreach(println)

    sc.stop()
  }

}
