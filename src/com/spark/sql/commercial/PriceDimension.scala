package com.spark.sql.commercial

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.{Map,ListBuffer}
import org.apache.spark.sql.functions._
import java.sql.{Connection,Statement,DriverManager,SQLException}
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.types.{StructField,StringType,StructType,DecimalType}

class PriceDimension(
    val sparkSession: SparkSession,
    val optionsMap: Map[String,String]) {
    
    val spark = sparkSession
    val basicObj = new BasicParameter(sparkSession,optionsMap)
    basicObj.pumpPartCommercialProduct("id", "100")
    val options = basicObj.options
    def getMinProductPrice(): Unit = {
      val url = options("url"); val user = options("user"); val password = options("password")
      val productDf = spark.read.format("jdbc").options(options).load
      //过滤无效数据
      val filterMap = Map(
          "productStatus" -> List("1"),
          "originalSource" -> List("01","02","03"),
          "partPrice" -> List(0))
      val broadcastFilterMap = spark.sparkContext.broadcast(filterMap)
      val validProductDf = productDf
        .filter{row => {
          val queryMap = broadcastFilterMap.value
          if (queryMap("productStatus").size > 0 && !filterMap("productStatus").contains(row.getAs[String]("status"))){
            false
          }
          else if (queryMap("originalSource").size > 0 && !filterMap("originalSource").contains(row.getAs[String]("originSource"))) {
            false
          }
          else if (queryMap("partPrice").size > 0 && row.getAs[Int]("partsPrice") == filterMap("partPrice")(0)){
            false
          }
          else {
            true
          }
        }}
      
      //将DataFrame转化为RDD，并将品质聚合为原厂、品牌
      import spark.implicits._
      val brandGroupedRdd = validProductDf
        .select("carTypeBrandName", "qualityType","partsOe","provinceCode","partsPrice")
        .rdd.map(row =>{
          var quality: String = ""
          val brand = row.getAs[String]("carTypeBrandName")
          val qual = row.getAs[String]("qualityType")
          val partOe = row.getAs[String]("partsOe")
          val province = row.getAs[String]("provinceCode")
          val partPrice = row.getAs[java.math.BigDecimal]("partsPrice")
          if(List("原厂件","流通原厂件").contains(qual))
            quality = "原厂件"
          else
            quality = "品牌件"
          RowFactory.create(brand,quality,partOe,province,partPrice)
        })
        
      //将RDD转换为DataFrame
      val structFields = Array(
          StructField("carTypeBrandName",StringType,true),
          StructField("qualityType",StringType,true),
          StructField("partsOe",StringType,true),
          StructField("provinceCode",StringType,true),
          StructField("partsPrice",DecimalType(10,2),true))
      
      val brandGroupedQualityDf = spark.createDataFrame(brandGroupedRdd, StructType(structFields))
        
      //获取品牌+品质+OE聚合，并获取最低价和平均价
      val brandQualityOeDf = brandGroupedQualityDf.groupBy("carTypeBrandName", "qualityType","partsOe","provinceCode")
        .agg(min("partsPrice") as "minPartPrice", avg("partsPrice") as "averagePartPrice")
        
      
        
        /*.foreachPartition(partition => {
          val list = new ListBuffer[Tuple6[String,String,String,String,BigDecimal,BigDecimal]]
          partition.foreach(row => {
            val brand = row.getAs[String]("carTypeBrandName")  //品牌
            val quality = row.getAs[String]("qualityType")  //配件品质
            val partOe = row.getAs[String]("partsOe")  //oe
            val province = row.getAs[String]("provinceCode")  //省份
            val minPrice = row.getAs[java.math.BigDecimal]("minPartPrice")  //最低价
            val avgPrice = row.getAs[java.math.BigDecimal]("averagePartPrice")  //平均价
            list.append((brand,quality,partOe,province,minPrice,avgPrice))
          })
          var conn: Connection = null
          var stmt: Statement = null
          try{
            conn = DriverManager.getConnection(url,user,password)
            stmt = conn.createStatement
            list.foreach(l=>{
                val sql = (""
                 + "INSERT INTO census_part_price(brand_name,quality,part_oe,province,min_price,avg_price,create_time,update_time) "
                 + "VALUES ('" + l._1 + "','" + l._2 + "','" + l._3 + "','" + l._4 + "'," + l._5 + "," + l._6 + ",NOW(),NOW()) "
                 + "ON DUPLICATE KEY UPDATE min_price = " + l._5 + ", avg_price = " + l._6 + ", update_time = NOW()")
                stmt.executeUpdate(sql)
            })
          }catch{
            case e: SQLException => e.printStackTrace()
          }finally{
            if(stmt != null){
              stmt.close
            }
            if(conn != null){
              conn.close
            }
          }
        })*/
    }
}