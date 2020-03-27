package com.spark.sql.localprice

import scala.collection.mutable.Map
import java.sql.{Connection,Statement,SQLException,DriverManager,ResultSet}

class MysqlOperation(val options: Map[String,String]) {
  var conn: Connection = null
  var stmt: Statement = null
  
  def createConnection(): Unit = {
     conn = DriverManager.getConnection(options("url"),options("user"),options("password"))
  }
  
  def closeConnection(): Unit = {
    if(stmt != null){
      stmt.close()
    }
    if(conn != null){
      conn.close()
    }
  }
  
  //查询操作
  def selectOp(sql: String): ResultSet = {
    var result: ResultSet = null
    try {
      stmt = conn.createStatement
      result = stmt.executeQuery(sql)
    } catch {
      case e: SQLException => e.printStackTrace() 
      case _: Exception => println("some errors happen when select data")
    }
    result
  }
  //插入操作
  def insertOps(sql: String): Unit = {
    try{
      stmt = conn.createStatement
      stmt.executeUpdate(sql)
    }catch {
      case e: SQLException => e.printStackTrace() 
      case _: Exception => println("some errors happen when insert data")
    }
  }
}