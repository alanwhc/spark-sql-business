package com.spark.sql.commercial

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import org.apache.spark.SparkConf

object RunApp {
  def main(args: Array[String]): Unit = {
    val env = args(0) //运行环境
    
    val conf = new SparkConf()
      .setAppName("CommericalGoodsCensus")
      .set("spark.default.parallelism", "36")
      
    val spark = SparkSession
      .builder()
      .config(conf)
   //   .master("local")
      .getOrCreate()
    
    var dbUrl: String = ""
    
    if(env == "local" || env == "test"){
       dbUrl = "jdbc:mysql://rm-uf6hnc20q03xba0l0ao.mysql.rds.aliyuncs.com:3306/jiaanpei_dataudit?rewriteBatchedStatements=true&serverTimezone=GMT"
     }else{
       dbUrl = "jdbc:mysql://rm-j5e4ss080mucdc9u0.mysql.rds.aliyuncs.com:3306/bbdv_dataudit?rewriteBatchedStatements=true&serverTimezone=GMT"
     }
    
    var options = Map(
        "url" -> dbUrl,
        "user" -> "bigdata",
        "password" -> "Bigdata1234",
        "driver" -> "com.mysql.cj.jdbc.Driver",
        "batchsize" -> "10000")
    
    //val provinceDimensionObj = new ProvinceDimension(spark,options)
    //provinceDimensionObj.getCommercialProductNum
    val priceDimensionObj = new PriceDimension(spark,options)
    priceDimensionObj.getMinProductPrice
  }
}