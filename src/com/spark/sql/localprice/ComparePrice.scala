package com.spark.sql.localprice

import scala.collection.mutable.Map
import org.apache.spark.sql.{SparkSession,Row}
import java.sql.{Connection,Statement,SQLException,DriverManager,ResultSet}
import org.apache.spark.sql.functions._
import java.sql.Date
import java.text.SimpleDateFormat

class ComparePrice(
  val sparkSesion: SparkSession,
  val url: String = "",
  val provinceCode: String = "",
  val brandName: String = "") {

    def runApp(): Unit = {
      val options: Map[String,String] = Map(
          "url" -> url,
          "driver" -> "com.mysql.cj.jdbc.Driver",
          "user" -> "bigdata",
          "password" -> "Bigdata1234")
      val tableName: String = getTableName(sparkSesion,options,provinceCode,true)
      getMaintainBrandOeNum(sparkSesion, options, tableName)
    }  
   
   //获取需要查询的表名 
   private def getTableName(sparkSession: SparkSession, options: Map[String,String],provinceCode: String,isHistory: Boolean): String = {
      options += ("dbtable" -> "code_province_mapping")
      val provinceDf = sparkSession.read.format("jdbc").options(options).load()
      val provinceMap: Map[String,String] = provinceDf
        .select("province_code","province")
        .rdd
        .map(row => row.getAs("province_code").toString -> row.getAs("province").toString)
        .collectAsMap().asInstanceOf[scala.collection.mutable.HashMap[String,String]]

      if(!isHistory){
        "picc_local_price_" + provinceMap(provinceCode)
      }else{
        "picc_history_localprice_" + provinceMap(provinceCode)
      }
    }
     
   //按日期分组查询每次维护的品牌+数量
   private def getMaintainBrandOeNum(sparkSession: SparkSession, options: Map[String,String], tableName: String): Unit = {
     val prop =  new java.util.Properties
     val url: String = options("url")
     val user: String = options("user")
     val password: String = options("password")
     prop.setProperty("user", user)  //用户名
     prop.setProperty("password", password) //密码
     
     val lowerBound: Long = 1  //下边界
     var upperBound: Long = 0  //上边界取id最大值
     val numPartitions: Int = 100  //分区数
     
     val mysqlObj = new MysqlOperation(options)
     
     mysqlObj.createConnection
     val sql = "SELECT MAX(id) as max_id FROM " + tableName + ""
     val resultSet: ResultSet = mysqlObj.selectOp(sql)
     
     while (resultSet.next) {
        upperBound = resultSet.getLong("max_id")
     }
     
     mysqlObj.closeConnection()
     
     val dateBrandDf = sparkSession.read.jdbc(options("url"), tableName, "id", lowerBound, upperBound, numPartitions, prop)
     val province = provinceCode
     dateBrandDf.groupBy("modify_date")
       .agg(approx_count_distinct("standard_brand_code") as "no_brand",approx_count_distinct("price_pid") as "no_brand_oe")
       .foreach(dateBrandOe => {
           val modifyDate: String = dateBrandOe.getAs[Date]("modify_date").toString //更新数据日期
           val noBrand: Long = dateBrandOe.getAs[Long]("no_brand") //更新品牌数
           val noBrandOe: Long = dateBrandOe.getAs[Long]("no_brand_oe")  //更新品牌+OE数量
         
           val sql: String = ("" 
             + "INSERT INTO census_date_brand_oe(date,province,no_brand,no_brand_oe,create_time,update_time) "
             + "VALUES (DATE_FORMAT('" + modifyDate + "','%Y-%m-%d')," + province + "," + noBrand + "," + noBrandOe + ", NOW(),NOW()) "
             + "ON DUPLICATE KEY UPDATE no_brand = " + noBrand + ",no_brand_oe = " + noBrandOe + ", update_time = NOW()")
          
           var conn: Connection = null
           var stmt: Statement = null
           try {
             conn = DriverManager.getConnection(url,user,password)
             stmt = conn.createStatement
             stmt.executeUpdate(sql)
           } catch {
             case e: SQLException => e.printStackTrace() 
             case _: Exception => println("some errors happen when insert data")
           } finally {
             if(stmt != null){
               stmt.close()
             }
             if(conn != null){
               conn.close()
             }
           }
             
       })
   }
   
}