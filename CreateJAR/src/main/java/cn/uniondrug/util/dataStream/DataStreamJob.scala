package cn.uniondrug.util.dataStream

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.slf4j.{Logger, LoggerFactory}

/**
 * @motto ：不忘初心
 * @author ：gjq
 * @date ：Created in 2020/3/21 15:30
 * @Function :
 */

object DataStreamJob {
  private val LOG = LoggerFactory.getLogger(DataStreamJob.getClass)
  private final val BOOTSTRAP_SERVERS = "bootstrap.servers"
  private final val GROUP_ID = "group.id"
  private final val TOPIC = "topic"

  def main(args: Array[String]): Unit = {
    var str = ParameterTool.fromArgs(args).get("config")

    if (str == null) {
      str = "E:\\work\\CreateJAR\\src\\main\\resources\\mysql.properties"
    }

    val tool = ParameterTool.fromPropertiesFile(str)

    //创建环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val pop = new Properties()
    pop.setProperty(BOOTSTRAP_SERVERS, tool.get(BOOTSTRAP_SERVERS))
    pop.setProperty(GROUP_ID, tool.get(GROUP_ID))

    val input: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](tool.get(TOPIC), new SimpleStringSchema(), pop)
    //接收流数据
    var kafkaSources: DataStream[String] = environment.addSource(input).name("kafkaSources")

    //val sdf = new SimpleDateFormat("yyyy-MM-dd")
    //数据处理
    kafkaSources
      .filter(new FilterFunction[String] {
        override def filter(value: String): Boolean = {
          if(value.nonEmpty){//如果数据不是空，执行下面
            if(value.contains("gjq")||value.contains("qiqi") || value.equals(" ") || value.equals(null)) {
              false
            }else{
              true
            }}else{
            false
          }
        }
      })
      .map(_.split("\\s+"))
      .map(st => Student(st(0).toInt, st(1), st(2), st(3)))
      .addSink(new UserDefineRichSinkFunction)


    //程序执行
    environment.execute("DataStreamJob")

  }
}
