package cn.uniondrug.util.flinkjson


import java.util
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.util.control.Breaks._
import scala.tools.nsc.doc.Universe
import scala.tools.nsc.doc.model.{Annotation, Entity, TemplateEntity}




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
    var kafkaSources: DataStream[String] = environment.addSource(input).setParallelism(1).name("JSON_process")

    //val sdf = new SimpleDateFormat("yyyy-MM-dd")
    //数据处理(处理简单的对象)
    kafkaSources.map(line => {
      val nObject = JSON.parseObject(line)
      val str1 = nObject.getString("id")
      val str2 = nObject.getString("username")
      val str3 = nObject.getString("password")
      val str4 = nObject.getString("gmtUpdated")
      employee(str1.toInt,str2,str3,str4)

    }).addSink(new UserDefineRichSinkFunction2)


    //程序执行
    environment.execute("DataStreamJob")

  }

}
