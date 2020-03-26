package cn.uniondrug.util

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
 * @motto ：不忘初心
 * @author ：gjq
 * @date ：Created in 2020/3/20 11:07
 * @Function :
 */

object QuickStart {
  def main(args: Array[String]): Unit = {
    //创建环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    //创建流
    val ds: DataStream[String] = environment.socketTextStream("Spark", 9999)
    val ds2: DataStream[String] = environment.socketTextStream("Spark", 8888)

    //数据转换(implicits隐含的)
    ds.filter(!_.isEmpty)
      .flatMap(str =>{str.split("\\s+")})
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .print()

    ds2.map(x => {
      x.toLowerCase
    }).print()

    //执行计算

    environment.execute("quickstart")
  }

}
