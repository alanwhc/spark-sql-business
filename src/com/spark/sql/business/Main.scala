package com.spark.sql.business

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import java.text.SimpleDateFormat
import java.util.Calendar

object Main {
  def main(args: Array[String]){
    val env = args(0)
    val dt = args(1)
    
    val conf = new SparkConf()
      .setAppName("BusinessData")
      .set("spark.default.parallelism", "48")
      .set("spark.sql.shuffle.partitions","50")
 
    var spark: SparkSession =  null
    
    var dbUrl1: String = ""; var dbUrl2: String = ""; var dbUrl3: String = ""
    var user: String = ""
    var password: String = ""
    
    if(env == "local"){
      dbUrl1 = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com/jiaanpei_report_db?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      dbUrl2 = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com/salesdb?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      dbUrl3 = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com/usersdb?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      user = "bigdata"
      password = "Bigdata1234"
      spark = SparkSession
        .builder
        .master("local")
        .config(conf)
        .getOrCreate
    }
    else if(env == "test"){
      dbUrl1 = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com/jiaanpei_report_db?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      dbUrl2 = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com/salesdb?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      dbUrl3 = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com/usersdb?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      user = "bigdata"
      password = "Bigdata1234"
      spark = SparkSession
        .builder
        .config(conf)
        .getOrCreate
    }
    else{
      dbUrl1 = "jdbc:mysql://rm-j5e2v8ius50974f67.mysql.rds.aliyuncs.com/jiaanpei_report_db?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      dbUrl2 = "jdbc:mysql://rm-j5e2v8ius50974f67.mysql.rds.aliyuncs.com/salesdb?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      dbUrl3 = "jdbc:mysql://rm-j5e2v8ius50974f67.mysql.rds.aliyuncs.com/usersdb?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai"
      user = "super_dbm"  
      password = "Whc910131"
      spark = SparkSession
        .builder
        .config(conf)
        .getOrCreate
    }
    
    var options = Map(
        "user" -> user,
        "password" -> password,
        "driver" -> "com.mysql.cj.jdbc.Driver",
        "batachsize" -> "10000"
        )

    var dateFormat: SimpleDateFormat = null
    var date: String = ""
    val urls = List(dbUrl1,dbUrl2,dbUrl3)
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
    
    val businessDailyObj = new BusinessDailyData(spark,options,dates,urls)
    businessDailyObj.deliveredOrderData
    businessDailyObj.vehiclePartOrderData
    businessDailyObj.repeatPurchaseShop(500)
    
    val managerAchievementObj = new ManagerAchievement(spark,options,dates,urls)
    managerAchievementObj.managerAchievement
    managerAchievementObj.marriedServiceCharge

    val salesBonusFactorObj = new SalesBonusFactor(spark,options,dates,urls)
    salesBonusFactorObj.salesBonusFactor
  }
}