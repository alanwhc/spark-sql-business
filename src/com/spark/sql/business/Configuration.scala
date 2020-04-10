package com.spark.sql.business

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import org.apache.spark.sql.functions._
import java.sql.{Connection,Statement,DriverManager,SQLException,ResultSet,Date}

class Configuration{
    
    //获取UpperBound
    def getUpperBound(options: Map[String,String]): Long = {
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
    
    //获取日期
    def getDateValue(options: Map[String,String],date: String, _type: String): Date = {
      var conn: Connection = null
      var stmt: Statement = null
      var resultDate: Date = null
      var result: ResultSet = null
      try{
        var sql: String = ""
        if(_type == "thismonthlastday")
          sql = "SELECT last_day('" + date + "') AS target_day"
        else if(_type == "lastmonthlastday")
          sql = "SELECT last_day(date_add('" + date + "',INTERVAL -1 MONTH)) AS target_day"
        else if(_type == "firstdayinmonth")
          sql = "SELECT date_format('" + date + "', '%Y-%m-01') AS target_day"
        else if(_type == "firstdayinyear")
          sql = "SELECT date_format('" + date + "', '%Y-01-01') AS target_day"
        else
          sql = ""
        conn = DriverManager.getConnection(options("url"), options("user"), options("password"))
        stmt = conn.createStatement
        result = stmt.executeQuery(sql)
        while(result.next){
          resultDate = result.getDate("target_day")
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
      resultDate
    }
    
}