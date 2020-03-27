package com.spark.sql.business

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import java.text.SimpleDateFormat
import java.util.Calendar

object Main {
  def main(args: Array[String]){
    val env = args(0)
    
    val conf = new SparkConf()
      .setAppName("BusinessData")
      .set("spark.default.parallelism", "48")
      .set("spark.sql.shuffle.partitions","50")
 
    var spark: SparkSession =  null
    
    var dbUrl: String = ""
    var user: String = ""
    var password: String = ""
    
    if(env == "test"){
      dbUrl = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com/jiaanpei_report_db?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      user = "bigdata"
      password = "Bigdata1234"
      spark = SparkSession
        .builder
        .master("local")
        .config(conf)
        //.enableHiveSupport
        .getOrCreate
    }
    else{
      dbUrl = "jdbc:mysql://rm-j5e2v8ius50974f67.mysql.rds.aliyuncs.com/jiaanpei_report_db?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      user = "super_dbm"  
      password = "Whc910131"
      spark = SparkSession
        .builder
        .config(conf)
        //.enableHiveSupport
        .getOrCreate
    }
    
    var options = Map(
        "url" -> dbUrl,
        "user" -> user,
        "password" -> password,
        "driver" -> "com.mysql.cj.jdbc.Driver",
        "batachsize" -> "10000"
        )
    
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var calendar: Calendar = Calendar.getInstance
    calendar.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(calendar.getTime)
    dateFormat = new SimpleDateFormat("yyyy")
    val year = dateFormat.format(calendar.getTime)
    dateFormat = new SimpleDateFormat("MM")
    val month = dateFormat.format(calendar.getTime)
    dateFormat = new SimpleDateFormat("dd")
    val day = dateFormat.format(calendar.getTime) 
    
    val dates = List(yesterday,year,month,day)
    val businessDailyObj = new BusinessDailyData(spark,options,dates)
    businessDailyObj.deliveredOrderData
    businessDailyObj.vehiclePartOrderData
  }
}