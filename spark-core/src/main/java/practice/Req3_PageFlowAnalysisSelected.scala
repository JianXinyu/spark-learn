package practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Req3_PageFlowAnalysisSelected {

  def main(args : Array[String]) : Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1_HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    val actionRDD = sc.textFile("data/user_visit_action.txt")
    val actionDataRDD = actionRDD.map(
      action => {
        val datas = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    )
    actionDataRDD.cache()

    // TODO 统计指定的页面跳转
    // 1-2, 2-3, 3-4, 4-5, 5-6, 6-7
    val ids = List[Long](1,2,3,4,5,6,7)
    // tail 除了第一个元素的所有元素
    val okFlows = ids.zip(ids.tail)

    // 计算分母
    val denominatorRDD: Map[Long, Long] = actionDataRDD.filter(
      // 过滤不感兴趣的页面
      action => {
        // init 除了最后一个元素的所有元素
        ids.init.contains(action.page_id)
      }
    ).map(
      action => {
        (action.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap

    // 计算分子
    // 根据session分组
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionDataRDD.groupBy(_.session_id)
    // 根据访问时间排序(升序)
    val mvRDD: RDD[(String, List[((Long, Long), Int)])] = sessionRDD.mapValues(
      iter => {
        val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)
        val flowIds: List[Long] = sortList.map(_.page_id)
        // [1,2,3,4] => [1-2,2-3,3-4]
        // method 1: sliding
        // [1,2,3,4] => [1,2], [2,3], [3,4]
        // method 2: zip 相同位置的合并在一起
        // [1,2,3,4] zip with
        // [2,3,4] (flowIds.tail)
        val pageFlowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)

        // 过滤不感兴趣的跳转
        pageFlowIds.filter {
          tuple => {
            okFlows.contains(tuple)
          }
        }.map(
          tuple => {
            (tuple, 1)
          }
        )
      }
    )
    // ((p1,p2),1)
    val flatRDD: RDD[((Long, Long), Int)] = mvRDD.map(_._2).flatMap(list => list)
    // ((p1,p2),sum)
    val numeratorRDD: RDD[((Long, Long), Int)] = flatRDD.reduceByKey(_ + _)

    // 计算单跳转换率
    numeratorRDD.foreach{
      case ( (pageId1, pageId2), sum ) => {
        val denominator : Long = denominatorRDD.getOrElse(pageId1, 0L)
        println(s"page ${pageId1} to page ${pageId2}'s ratio is "  + sum.toDouble / denominator)
      }
    }


    sc.stop()
  }

  // 用户访问动作表
  case class UserVisitAction(
    date: String, // 用户点击行为的日期
    user_id: Long, // 用户的 ID
    session_id: String, //Session 的 ID
    page_id: Long, // 某个页面的 ID
    action_time: String, // 动作的时间点
    search_keyword: String, // 用户搜索的关键词
    click_category_id: Long, // 某一个商品品类的 ID
    click_product_id: Long, // 某一个商品的 ID
    order_category_ids: String, // 一次订单中所有品类的 ID 集合
    order_product_ids: String, // 一次订单中所有商品的 ID 集合
    pay_category_ids: String, // 一次支付中所有品类的 ID 集合
    pay_product_ids: String, // 一次支付中所有商品的 ID 集合
    city_id: Long // 城市 id
  )
}
