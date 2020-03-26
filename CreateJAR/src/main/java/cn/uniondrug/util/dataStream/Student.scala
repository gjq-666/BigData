package cn.uniondrug.util.dataStream

import java.util.Date

/**
 * @motto ：不忘初心
 * @author ：gjq
 * @date ：Created in 2020/3/22 11:59
 * @Function :
 */

//表中的日期类型，创建实体类时，可以将日期类型的字段设置为String类型(默认识别2020/3/22这种格式)
case class Student(id:BigInt,username:String,password:String,gmtUpdated:String) {
}
