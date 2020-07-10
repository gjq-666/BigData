package cn.uniondrug.util.list

import java.util
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import org.omg.CORBA.Any
import org.slf4j.LoggerFactory

import scala.util.control.Breaks._
import scala.xml.dtd.ANY


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

    //处理集合对象
    val value: DataStream[util.ArrayList[employee]] = kafkaSources.map(line => {
      var list = new util.ArrayList[employee]()
      var nObject = JSON.parseObject(line)
      val array: JSONArray = nObject.getJSONArray("resultData")

      //可以把java中的集合转化为Scala中的集合
      import scala.collection.JavaConversions._
      breakable {
        for (i <- 0 to array.size() - 1) {
          //获取到数组中的每一个对象
          val nObject1 = array.getJSONObject(i)
          val emp: employee = employee(nObject1.getString("id").toInt, nObject1.getString("username"), nObject1.getString("password"), nObject1.getString("gmtUpdated"))
          list.add(emp)

          if (i == array.size()) {
            break()
          }
        }
      }
      list
    })
    value.addSink(new UserDefineRichSinkFunction4)

    //程序执行
    environment.execute("DataStreamJob")

  }

}
