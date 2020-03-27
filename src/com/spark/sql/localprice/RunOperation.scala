package com.spark.sql.localprice

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map

class RunOperation(
  val sparkSession: SparkSession = null,
  val dbUrl: String = "",
  val provinceCode: String = "") {
    
    var spark: SparkSession = sparkSession
    var url: String = dbUrl
    var province: String = provinceCode
    var prop = new java.util.Properties
    var options: Map[String,String] = Map(
          "url" -> url,
          "driver" -> "com.mysql.cj.jdbc.Driver",
          "user" -> "bigdata",
          "password" -> "Bigdata1234")
    
    def runApp(): Unit = {
      //获取需要查询的表名
      val tableName = getTableName("history")
      
      //获取该表的上边界
      val upperBound = getUpperBound(tableName)

      //创建计算过程对象
      val compute = new ComputeProcess
      
      //统计每次更新的品牌数及不重复OE数量
      compute.getBrandOeNum(spark, url, options("user"), options("password"), tableName, "id", 1, upperBound, 100, prop, province)
    }
    
    private def getTableName(tableType: String): String = {
      options += ("dbtable" -> "code_province_mapping")
      val provinceDf = sparkSession.read.format("jdbc").options(options).load
      val provinceMap = provinceDf
        .select("province_code", "province")
        .rdd
        .map(row => row.getAs[String]("province_code") -> row.getAs[String]("province"))
        .collectAsMap.asInstanceOf[scala.collection.mutable.HashMap[String,String]]
      
      if(tableType == "current"){
        "picc_local_price_" + provinceMap(provinceCode)
      }
      else if(tableType == "history"){
        "picc_history_localprice_" + provinceMap(provinceCode)
      }
      else{
        ""
      }
    }
    
    private def getUpperBound(tableName: String): Long = {
      val url = options("url"); val user = options("user"); val password = options("password")
      prop.setProperty("user", user); prop.setProperty("password", password)
      var upperBound: Long = 0
      val mysqlObj = new MysqlOperation(options)
      mysqlObj.createConnection
      val sql = "SELECT MAX(id) AS max_id FROM " + tableName + ""
      val resultSet = mysqlObj.selectOp(sql)
      while (resultSet.next) {
        upperBound = resultSet.getLong("max_id")
       }
      mysqlObj.closeConnection
      upperBound
    }
    
  
}