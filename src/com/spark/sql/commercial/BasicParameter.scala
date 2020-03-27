package com.spark.sql.commercial

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import org.apache.spark.sql.functions._
import java.sql.{Connection,Statement,DriverManager,SQLException,ResultSet}

class BasicParameter(
    val sparkSession: SparkSession,
    val optionsMap: Map[String,String]) {
  
    val spark = sparkSession
    val options = optionsMap
    
    //全车件商品库
    def pumpPartCommercialProduct(
        columnName: String,
        numPartition: String):Unit = {
      options += ("dbtable" -> "janpb_goods_supp_standard_info")
      options += ("partitionColumn" -> columnName)
      options += ("lowerBound" -> "1")
      options += ("upperBound" -> getUpperBound.toString)
      options += ("numPartitions" -> numPartition)
    }
    
    //获取UpperBound
    private def getUpperBound: Long = {
      var conn: Connection = null
      var stmt: Statement = null
      var upperBound: Long = 0
      var result: ResultSet = null
      try{
        val sql = "SELECT max(id) as max_id FROM " + options("dbtable")
        conn = DriverManager.getConnection(options("url"), options("user"), options("password"))
        stmt = conn.createStatement
        result = stmt.executeQuery(sql)
        while(result.next){
          upperBound = result.getLong("max_id")
        }
      }catch{
        case e: SQLException => e.printStackTrace()
        case _: Exception => println("MySQL error when getting upper bound.")
      }finally{
        if(stmt != null){
          stmt.close
        }
        if(conn != null){
          conn.close
        }
      }
      upperBound
    }
}