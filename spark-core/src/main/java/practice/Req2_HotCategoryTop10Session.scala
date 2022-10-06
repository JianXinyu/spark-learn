package practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Req2_HotCategoryTop10Session {

  def main(args : Array[String]) : Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1_HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    val actionRDD = sc.textFile("data/user_visit_action.txt")
    actionRDD.cache()
    val top10Ids : Array[String] = Top10Category(actionRDD)

    // 1. 过滤原始数据，保留点击和前10品类ID
    val filterSessionID: RDD[String] = actionRDD.filter(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          top10Ids.contains(datas(6))
        } else {
          false
        }
      }
    )
    // 2. 根据 品类ID 和 seesion ID 进行统计
    val reduceRDD: RDD[((String, String), Int)] = filterSessionID.map(
      action => {
        val datas = action.split("_")
        ((datas(6), datas(2)), 1)
      }
    ).reduceByKey(_ + _)

    // 3. 将统计的结果进行结构转换
    //    ((品类ID，Session ID), sum) => (品类ID，(Session ID, sum))
    val mapRDD = reduceRDD.map {
      case ((cid, sid), sum) => {
        (cid, (sid, sum))
      }
    }

    // 4. 相同品类进行分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

    // 5. 将分组后的数据进行点击量排序，取Top10
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    )

    resultRDD.collect().foreach(println)

    sc.stop()
  }

  def Top10Category(actionRDD : RDD[String]) : Array[String] = {
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

    val analysisRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    
    analysisRDD.sortBy(_._2, false).take(10).map(_._1)
  }
}
