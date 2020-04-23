package com.spark.sql.localpart

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.Map

object Main {
  def main(args: Array[String]){
    val env = args(0)
    val dt = args(1)
    val targetProvince = args(2)
    val provinces = args(3)
    
    val conf = new SparkConf()
      .setAppName("LocalPartPrice")
      .set("spark.default.parallelism", "48")
      .set("spark.sql.shuffle.partitions","50")
   
    var spark: SparkSession = null
    var dbUrl = ""; var user = ""; var password = ""
    
    if(env == "local"){
      dbUrl = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com/piccclaimdb?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      spark = SparkSession.builder.master("local").config(conf).getOrCreate
    }
    else if(env == "test"){
      dbUrl = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com/piccclaimdb?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      spark = SparkSession.builder.config(conf).getOrCreate
    }
    else{
      dbUrl = "jdbc:mysql://rm-j5e4ss080mucdc9u0.mysql.rds.aliyuncs.com/piccclaimdb?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      spark = SparkSession.builder.config(conf).getOrCreate
    }
    
    val options: Map[String,String] = Map(
        "url" -> dbUrl,
        "user" -> "bigdata",
        "password" -> "Bigdata1234",
        "driver" -> "com.mysql.cj.jdbc.Driver",
        "batachsize" -> "10000")
        
    //获取时间、年月日等
    var dateFormat: SimpleDateFormat = null
    var date: String = ""
    
    if(dt == "yesterday"){
      dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val calendar: Calendar = Calendar.getInstance
      calendar.add(Calendar.DATE, -1)
      date = dateFormat.format(calendar.getTime)
    }else{
      date = dt
    }

    val year = date.split("-")(0)
    val month = date.split("-")(1)
    val day = date.split("-")(2)
    
    val dates = List(date,year,month,day)
    
    val priceMatchObj = new LocalPartPriceMatching(spark,options,dates,targetProvince,provinces)
    //priceMatchObj.addNewColumn
    priceMatchObj.priceMatching
  }
}