package com.spark.sql.localprice

import org.apache.spark.sql.SparkSession
import java.sql.{Connection,Statement,SQLException,DriverManager,ResultSet,Date}
import org.apache.spark.sql.functions._

class ComputeProcess {
  def getBrandOeNum(
    spark: SparkSession,
    url: String,
    user: String,
    password: String,
    tableName: String,
    columnName: String,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int,
    prop: java.util.Properties,
    province: String) = {
    
    val dateBrandDf = spark.read.jdbc(url, tableName, columnName, lowerBound, upperBound, numPartitions, prop)
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
  
  def brandVerticalComparison: Unit = {
    
  }
}