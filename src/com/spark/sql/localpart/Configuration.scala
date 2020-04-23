package com.spark.sql.localpart

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import org.apache.spark.sql.functions._
import java.sql.{Connection,Statement,DriverManager,SQLException,ResultSet,Date}
import scala.collection.mutable.ListBuffer
import java.sql.CallableStatement
import java.sql.Types

class Configuration(
    val url: String = "",
    val user: String = "",
    val password: String = "") extends Serializable {
    
    /**
     * 获取表的最大id
     */
    def getUpperBound(tableName: String, column: String): Long = {
      var conn: Connection = null
      var stmt: Statement = null
      var upperBound: Long = 0
      var result: ResultSet = null
      try{
        val sql = "SELECT max(" + column + ") as max_id FROM " + tableName
        conn = DriverManager.getConnection(url, user, password)
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
    
    /**
     * 获取品牌列表
     */
    def getBrandList(tableName: String) = {
      var conn: Connection = null; var stmt: Statement = null; var resultSet: ResultSet = null
      val result = new ListBuffer[String]
      try {
        conn = DriverManager.getConnection(url, user, password)
        stmt = conn.createStatement
        val sql = "SELECT DISTINCT standard_brand_code FROM " + tableName + ";"
        resultSet = stmt.executeQuery(sql)
        while(resultSet.next){
          result.append(resultSet.getString("standard_brand_code"))
        }
      } catch {
        case t: Throwable => t.printStackTrace() // TODO: handle error
      }
      result.toList
    }
    
    /**
     * 获取省份拼音、标准品牌编码
     */
    def getBrandCodeProvinceName(tableName: String, variable: String, queryType: String) = {
      var conn: Connection = null; var stmt: Statement = null; var resultSet: ResultSet = null
      var result: String = ""; var sql: String = ""
      try {
        conn = DriverManager.getConnection(url, user, password)
        stmt = conn.createStatement
        queryType match {
          //获取标准品牌名称
          case "GetStandardBrandCode" => sql = "SELECT standard_brand_code AS result FROM " + tableName + " WHERE standard_brand_name = '" + variable + "';"
          case "GetProvincePinYin" => sql = "SELECT province AS result FROM " + tableName + " WHERE province_code = '" + variable + "';"
          case _ => sql = ""
        }
        resultSet = stmt.executeQuery(sql)
        while(resultSet.next){
          result = resultSet.getString("result")
        }
      } catch {
        case t: Throwable => t.printStackTrace() // TODO: handle error
      }
      result
    }
    
    /**
     * 通过编码获取省份中文名称
     */
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
    
    /**
     * 通过编码获取省份中文名称
     */
    def getProvinceCHNNameByPinYin:(String => String) = (provinceName: String) => {
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
    
    /**
     * 增加列
     */
    def addColumn(tableName: String, columnName: String, columnType: String, defaultValue: String, columnComment: String, previousColumn: String){
      var conn: Connection = null
      var stmt: Statement = null
      try {
        conn = DriverManager.getConnection(url, user, password)
        stmt = conn.createStatement
        val sql = (""
              + "ALTER TABLE " + tableName + " "
              + "ADD COLUMN " + columnName + " " + columnType + " " 
              + "DEFAULT '" + defaultValue + "' COMMENT '" + columnComment + "' AFTER " + previousColumn + ";")
        println(sql)
        stmt.execute(sql)
      } catch {
        case e: SQLException => e.printStackTrace()
      } finally{
        if(stmt != null)
          stmt.close
        if(conn != null)
          conn.close
      }
    }
    
    /**
     * 删除列
     */
    def dropColumn(tableName: String, columnName: String){
      var conn: Connection = null
      var stmt: Statement = null
      var cstmt: CallableStatement = null
      try {
        conn = DriverManager.getConnection(url, user, password)
        stmt = conn.createStatement
        var sql = "{? = CALL func_exist_column(?,?)}"
        cstmt = conn.prepareCall(sql)
        cstmt.registerOutParameter(1, Types.VARCHAR)
        cstmt.setString(2, tableName)
        cstmt.setString(3, columnName)
        cstmt.execute
        val result = cstmt.getString(1)
        
        if(result.equals("1")){
          sql = "ALTER TABLE " + tableName + " DROP COLUMN " + columnName + ";"
          stmt.execute(sql)
        }
      } catch {
        case e: SQLException => e.printStackTrace()
      } finally{
        if(stmt != null)
          stmt.close
        if(cstmt != null)
          cstmt.close
        if(conn != null)
          conn.close
      }
    }
}