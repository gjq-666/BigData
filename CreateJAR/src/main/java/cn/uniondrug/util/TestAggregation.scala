package cn.uniondrug.util

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
/**
 * @motto ：不忘初心
 * @author ：gjq
 * @date ：Created in 2020/3/21 12:17
 * @Function :
 */

object TestAggregation {
  def main(args: Array[String]): Unit = {
    //创建环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    //创建数据流
    val ds: DataStream[String] = environment.socketTextStream("Spark", 9999)

    //数据转换(格式  15 gjq 技术开发 13000)
    ds
      .map(_.split(" "))
      .map(t => Employee(t(0),t(1),t(2),t(3).toDouble))
      .keyBy("dept")
      .maxBy("salary") //取最大值的这一行元素
      //.max("salary")  //只拿最大值，
      .print()

    //执行计算
    environment.execute("Aggregation")
  }
}
