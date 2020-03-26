package cn.uniondrug.util.dataStream

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction


/**
 * @motto ：不忘初心
 * @author ：gjq
 * @date ：Created in 2020/3/21 16:24
 * @Function :
 *
 **/
class UserDefineRichSinkFunction extends RichSinkFunction[Student] {
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

  //反复调用的
  override def invoke(student: Student) : Unit = {
    val sql: String = "insert into student(username,password,gmtUpdated) values(?,?,?)"
    prep = conn.prepareStatement(sql)
    //主键自增，不需要设置
    //prep.setString(0,student.id.t)
    prep.setString(1, student.username)
    prep.setString(2, student.password)
    prep.setString(3,student.gmtUpdated)
    prep.execute()
    conn.commit()
  }

  //关闭
  override def close(): Unit = {
    super.close()
    if (conn != null) {
      conn.close()
    }
  }
}

