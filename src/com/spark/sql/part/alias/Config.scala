package com.spark.sql.part.alias

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import org.apache.spark.sql.functions._
import java.sql.{Connection,Statement,DriverManager,SQLException,ResultSet,Date}

class Config(val url1: String = "", val user1: String = "", val password1: String = "") extends Serializable{
    private val url = url1; private val user = user1; private val password = password1
    
    /**
     * 获取UpperBound
     */
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
    
    /**
     * 去除特殊字符函数
     */
    def removeSpecialCharacter:(String => String) = (character: String) =>{
      character.replaceAll("""([+-/~.# ]|\\[s*])""", "")
    }
}