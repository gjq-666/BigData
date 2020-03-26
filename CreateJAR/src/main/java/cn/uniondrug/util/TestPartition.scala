package cn.uniondrug.util

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
 * @motto ：不忘初心
 * @author ：gjq
 * @date ：Created in 2020/3/21 12:44
 * @Function :
 */

object TestPartition {
  def main(args: Array[String]): Unit = {

    //创建环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    //创建数据流
    val ds = environment.socketTextStream("Spark", 9999)

    //数据处理
    ds.flatMap(_.split("\\s+"))
        .map((_,1))
        .partitionCustom(new Partitioner[String] {
          override def partition(key: String, numPartitions: Int): Int = {

           var p = (key.hashCode&Integer.MAX_VALUE)%numPartitions
            println("key"+key+",numPartitions"+numPartitions+p)
            p
          }
        },0)
        .print()

    //执行计算
    environment.execute("TestPartition")
  }
}
