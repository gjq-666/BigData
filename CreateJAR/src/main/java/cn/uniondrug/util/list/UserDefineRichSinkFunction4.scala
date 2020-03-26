package cn.uniondrug.util.list

import java.util.Properties
import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.omg.CORBA.Any
import org.slf4j.LoggerFactory

import scala.util.control.Breaks._
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import scala.collection.JavaConversions._

/**
 * @motto ：不忘初心
 * @author ：gjq
 * @date ：Created in 2020/3/21 16:24
 * @Function :
 *
 **/
class UserDefineRichSinkFunction4 extends RichSinkFunction[util.ArrayList[employee]] {
  var conn: Connection = null
  var prep: PreparedStatement = null
  val url: String = "jdbc:mysql://Spark:3306/test"
  val jdbc: String = "com.mysql.jdbc.Driver"
  val username: String = "root"
  val password: String = "root"

  //初始化
  override def open(parameters: Configuration): Unit = {


    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection(url, username, password)
    conn.setAutoCommit(false)
  }

  override def close(): Unit = {
    super.close()
    if (conn != null) {
      conn.close()
    }
  }

  //反复调用的
  override def invoke(array: util.ArrayList[employee]): Unit = {

    breakable {
      for (i <- 0 to array.size() - 1) {
        val emp: employee = employee(array(i).id, array(i).username, array(i).password, array(i).gmtUpdated)

        val sql: String = "insert into employee(username,password,gmtUpdated) values(?,?,?)"
        prep = conn.prepareStatement(sql)
        //主键自增，不需要设置
        //prep.setString(0,student.id.t)
        prep.setString(1, emp.username)
        prep.setString(2, emp.password)
        prep.setString(3, emp.gmtUpdated)
        prep.execute()

        if (i == array.size()) {
          break()
        }
        conn.commit()
      }
    }
  }
}

