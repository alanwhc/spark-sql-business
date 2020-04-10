package com.spark.sql.part

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
    val brands = args(3)  //需要比对的品牌
    val provinces = args(4)  //需要比对价格的省份
    val conf = new SparkConf()
      .setAppName("PartAnalysis")
      .set("spark.default.parallelism", "48")
      .set("spark.sql.shuffle.partitions","50")
      
    var spark: SparkSession = null
    
    var dbUrl1: String = ""; var dbUrl2: String = ""
    var user1: String = ""; var user2: String = ""
    var password1: String = ""; var password2: String = ""
    
    if(env == "local"){
      dbUrl1 = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com/jiaanpei_report_db?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      dbUrl2 = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com/piccclaimdb?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      user1 = "bigdata"; user2 = "bigdata"
      password1 = "Bigdata1234"; password2 = "Bigdata1234"
      spark = SparkSession
        .builder
        .master("local")
        .config(conf)
        .getOrCreate
    }
    else if(env == "test"){
      dbUrl1 = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com/jiaanpei_report_db?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      dbUrl2 = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com/piccclaimdb?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      user1 = "bigdata"; user2 = "bigdata"
      password1 = "Bigdata1234"; password2 = "Bigdata1234"
      spark = SparkSession
        .builder
        .config(conf)
        .getOrCreate
    }
    else{
      dbUrl1 = "jdbc:mysql://rm-j5e2v8ius50974f67.mysql.rds.aliyuncs.com/jiaanpei_report_db?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      dbUrl2 = "jdbc:mysql://rm-j5e4ss080mucdc9u0.mysql.rds.aliyuncs.com/piccclaimdb?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      user1 = "super_dbm"; user2 = "bigdata"
      password1 = "Whc910131"; password2 = "Bigdata1234"
      spark = SparkSession
        .builder
        .config(conf)
        .getOrCreate
    }
    
    val options: Map[String,String] = Map(
        "driver" -> "com.mysql.cj.jdbc.Driver",
        "batachsize" -> "10000"
        )
    
    //用户名、密码、url
    val users = List(user1,user2)
    val passwords = List(password1,password2)
    val urls = List(dbUrl1,dbUrl2)
    
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

    val configs = Map("url" -> urls, "user" -> users, "password" -> passwords)
    
    val minPriceObj = new JapInquiryOrderMinPrice(spark,options,dates,configs)
    minPriceObj.calculatePartMinPrice
    
    //val priceMatchingObj = new LocalPriceMatching(spark,options,dates,configs,targetProvince,brands,provinces)
    //priceMatchingObj.priceMatching
  }
}