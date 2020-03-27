package com.spark.sql.localprice

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
     val env: String = args(0)  //运行环境
     val provinceCode: String = args(1)  //省份代码
     //val brandName: String = args(2)  //品牌名称
     //val date: String = args(2)  //日期
     
     var conf: SparkConf = null
     var dbUrl: String = ""
     if(env == "local"){
       conf = new SparkConf()
         .setAppName("PiccLocalPrice")
         .setMaster("local")
       dbUrl = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com:3306/piccclaimdb"
     }else if (env == "test") {
       conf = new SparkConf()
         .setAppName("PiccLocalPrice")
       dbUrl = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com:3306/piccclaimdb"
     }else{
       conf = new SparkConf()
         .setAppName("PiccLocalPrice")
       dbUrl = "jdbc:mysql://rm-j5e4ss080mucdc9u0.mysql.rds.aliyuncs.com:3306/piccclaimdb"
     }
    
     val sparkSession = SparkSession
       .builder()
       .config(conf)
       .getOrCreate()
       
     val runApp = new RunOperation(sparkSession,dbUrl,provinceCode)
     runApp.runApp()
     //val compareObj: ComparePrice = new ComparePrice(sparkSession,dbUrl,provinceCode) 
     //compareObj.runApp()
    
  }
}