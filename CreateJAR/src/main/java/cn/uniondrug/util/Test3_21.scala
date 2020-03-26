package cn.uniondrug.util

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner

/**
 * @motto ：不忘初心
 * @author ：gjq
 * @date ：Created in 2020/3/21 10:13
 * @Function :
 */

object Test3_21 {
  def main(args: Array[String]): Unit = {

    //创建环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    //获取数据流
    val ds: DataStream[String] = environment.socketTextStream("Spark", 9999)

    //数据处理  reduce
    /*ds.flatMap(_.split("\\s+"))
      .map(WordPair(_, 1))
      .keyBy("key")
      .reduce((v1, v2) => WordPair(v1.key, v1.count + v2.count))
      .print()*/


    //触发程序执行
    environment.execute("Test3_21")
  }

}
