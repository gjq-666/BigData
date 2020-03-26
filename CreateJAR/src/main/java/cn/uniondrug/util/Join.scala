package cn.uniondrug.util

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.runtime.streamrecord.StreamElement
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @motto ：不忘初心
 * @author ：gjq
 * @date ：Created in 2020/3/20 13:36
 * @Function : Join
 */

object Join {
  def main(args: Array[String]): Unit = {

    //创建环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val ds: DataStream[String] = environment.socketTextStream("Spark", 9999)
    val ds2: DataStream[String] = environment.socketTextStream("Spark", 8888)

    //jion操作
    /*ds.connect(ds2)
      .flatMap(
        (line:String)=>line.split("\\s+"),
        (line:String)=>line.split("\\s+"))
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .print()*/
    val error: OutputTag[String] = new OutputTag[String]("error")

    //process方法的使用
    val dataStream: DataStream[String] = ds.process(new ProcessFunction[String, String] {

      //value ：接受到的流数据   ctx:错误时输出的数据   out：正确时输出的数据
      override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {

        if (value.startsWith("INFO")) {
          out.collect(value)

        } else {
          ctx.output(error, value)

        }

      }
    })
    dataStream.print("正常信息")
    dataStream.getSideOutput(error).print("错误信息")

    //执行操作
    environment.execute("jion")
  }
}
