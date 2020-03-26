package cn.uniondrug.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamContextEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
 * @motto ：不忘初心
 * @author ：gjq
 * @date ：Created in 2020/3/20 17:35
 * @Function :
 */

object ValueStage {
  def main(args: Array[String]): Unit = {

    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[String] = environment.socketTextStream("Spark", 9999)

    //对数据做转换
    ds.flatMap(_.split("\\s+"))
      .map((_, 1))
      .keyBy(0)
      .map(new RichMapFunction[(String, Int), (String, Int)] {
        var valueState: ValueState[Int] = _

        override def open(parameters: Configuration): Unit = {
          val vsd = new ValueStateDescriptor[Int]("wordcount", createTypeInformation[Int])
          valueState = getRuntimeContext.getState(vsd)
        }

        override def map(value: (String, Int)): (String, Int) = {
          var i = valueState.value()
          if (i == null) {
            i = 0
          }
          //更新历史
          valueState.update(i + value._2)
          (value._1, valueState.value())


        }
      }).print()

    //触发程序执行
    environment.execute("ValueStage")
  }

}
