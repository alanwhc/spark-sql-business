package com.spark.sql.part

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import org.apache.spark.sql.functions._
import java.sql.{Connection,Statement,DriverManager,SQLException,ResultSet,Date}

class Config(val url1: String = "", val user1: String = "", val password1: String = "") extends Serializable{
    private val url = url1; private val user = user1; private val password = password1
    //获取UpperBound
    def getUpperBound(options: Map[String,String],column: String): Long = {
      var conn: Connection = null
      var stmt: Statement = null
      var upperBound: Long = 0
      var result: ResultSet = null
      try{
        val sql = "SELECT max(" + column + ") as max_id FROM " + options("dbtable")
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
        else if(_type == "todayOfLastYear")
          sql = "SELECT date_add('" + date + "', INTERVAL -1 YEAR) AS target_day"
        else if(_type == "dayOfLastQuarter")
          sql = "SELECT date_add('" + date + "', INTERVAL -90 DAY) AS target_day"
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
    
    def getBrandCode(url:String, user:String, password: String, table: String, brandName: String) = {
      var conn: Connection = null
      var stmt: Statement = null
      var result: ResultSet = null
      var brandCode: String = ""
      try{
        conn = DriverManager.getConnection(url, user, password)
        stmt = conn.createStatement
        val sql = "SELECT standard_brand_code FROM " + table + " WHERE standard_brand_name = '" + brandName + "'"
        result = stmt.executeQuery(sql)
        while(result.next){
          brandCode = result.getString("standard_brand_code")
        }
      }catch{
        case e: SQLException => e.printStackTrace
      }finally{
        if(stmt != null)
          stmt.close
        if(conn != null)
          conn.close
      }
      brandCode
    }
    
    def getProvinceName(url:String, user:String, password: String, table: String, provinceCode: String) = {
      var conn: Connection = null
      var stmt: Statement = null
      var result: ResultSet = null
      var province: String = ""
      try{
        conn = DriverManager.getConnection(url, user, password)
        stmt = conn.createStatement
        val sql = "SELECT province FROM " + table + " WHERE province_code = '" + provinceCode + "'"
        result = stmt.executeQuery(sql)
        while(result.next){
          province = result.getString("province")
        }
      }catch{
        case e: SQLException => e.printStackTrace
      }finally{
        if(stmt != null)
          stmt.close
        if(conn != null)
          conn.close
      }
      province
    }
    
    def insertIntoTable(url: String, user: String, password: String, sql: String){
      var conn: Connection = null
      var stmt: Statement = null
      try{
        conn = DriverManager.getConnection(url, user, password)
        stmt = conn.createStatement
        stmt.execute(sql)
      }catch{
        case e: SQLException => e.printStackTrace
      }finally{
        if(stmt != null)
          stmt.close
        if(conn != null)
          conn.close
      }
    }
    
    def getProvinceCHNNameByCode:(String => String) = (provinceCode: String) => {
      var conn: Connection = null
      var stmt: Statement = null
      var result: ResultSet = null
      var province: String = ""
      try{
        conn = DriverManager.getConnection(url, user, password)
        stmt = conn.createStatement
        val sql = "SELECT province_chn FROM code_province_mapping WHERE province_code = '" + provinceCode + "'"
        result = stmt.executeQuery(sql)
        while(result.next){
          province = result.getString("province_chn")
        }
      }catch{
        case e: SQLException => e.printStackTrace
      }finally{
        if(stmt != null)
          stmt.close
        if(conn != null)
          conn.close
      }
      province
    }
    
    def getProvinceCHNNameByName:(String => String) = (provinceName: String) => {
      var conn: Connection = null
      var stmt: Statement = null
      var result: ResultSet = null
      var province: String = ""
      try{
        conn = DriverManager.getConnection(url, user, password)
        stmt = conn.createStatement
        val sql = "SELECT province_chn FROM code_province_mapping WHERE province = '" + provinceName + "'"
        result = stmt.executeQuery(sql)
        while(result.next){
          province = result.getString("province_chn")
        }
      }catch{
        case e: SQLException => e.printStackTrace
      }finally{
        if(stmt != null)
          stmt.close
        if(conn != null)
          conn.close
      }
      province
    }
    
}