package com.spark.sql.part.alias

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.Map
object Main {
  def main(args: Array[String]){
    val env = args(0)  //运行环境
    val dt = args(1)  //日期
    val conf = new SparkConf()
      .setAppName("PartAlias")
      .set("spark.default.parallelism", "48")
      .set("spark.sql.shuffle.partitions","50")
      
    var spark: SparkSession = null
   
    var dbUrl1: String = ""; var dbUrl2: String = ""; var dbUrl3: String = ""
    var user1: String = ""; var user2: String = ""; var user3: String = ""
    var password1: String = ""; var password2: String = ""; var password3: String = ""
   
    if(env == "local"){
      dbUrl1 = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com/jiaanpeidev?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      dbUrl2 = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com/part?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      dbUrl3 = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com/jiaanpei_dataudit?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      user1 = "bigdata"; user2 = "bigdata"; user3 = "bigdata"
      password1 = "Bigdata1234"; password2 = "Bigdata1234"; password3 = "Bigdata1234"
      spark = SparkSession
        .builder
        .master("local")
        .config(conf)
        .getOrCreate
    }
    else if(env == "test"){
      dbUrl1 = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com/jiaanpeidev?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      dbUrl2 = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com/part?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      dbUrl3 = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com/jiaanpei_dataudit?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      user1 = "bigdata"; user2 = "bigdata"; user3 = "bigdata"
      password1 = "Bigdata1234"; password2 = "Bigdata1234"; password3 = "Bigdata1234"
      spark = SparkSession
        .builder
        .config(conf)
        .getOrCreate
    }
    else{
      dbUrl1 = "jdbc:mysql://rm-j5e2v8ius50974f67.mysql.rds.aliyuncs.com/jiaanpeidb?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      dbUrl2 = "jdbc:mysql://rm-j5e4ss080mucdc9u0.mysql.rds.aliyuncs.com/part?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      dbUrl3 = "jdbc:mysql://rm-j5e4ss080mucdc9u0.mysql.rds.aliyuncs.com/bbdv_dataudit?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"      
      user1 = "super_dbm"; user2 = "bigdata"; user3 = "bigdata"
      password1 = "Whc910131"; password2 = "Bigdata1234"; password3 = "Bigdata1234"
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
    val users = List(user1,user2,user3)
    val passwords = List(password1,password2,password3)
    val urls = List(dbUrl1,dbUrl2,dbUrl3)
    
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

    val inqPartObj = new InquiryPart(spark,options,dates,configs)
    inqPartObj.runApp
    
    val goodsPartObj = new GoodsPart(spark,options,dates,configs)
    goodsPartObj.runApp
  }
}