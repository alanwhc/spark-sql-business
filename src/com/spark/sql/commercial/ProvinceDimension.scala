package com.spark.sql.commercial

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.{Map,ListBuffer}
import org.apache.spark.sql.functions._
import java.sql.{Connection,Statement,DriverManager,SQLException,ResultSet}

class ProvinceDimension(
    val sparkSession: SparkSession = null,
    val optionsMap: Map[String,String] = Map()
    ){
    
    val basicObj = new BasicParameter(sparkSession,optionsMap)
    basicObj.pumpPartCommercialProduct("id", "150")
    val options = basicObj.options
    val spark = sparkSession
    
    //获取不同渠道商品总数
    def getCommercialProductNum(): Unit = {
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

      //将过滤后的DataFrame转化为rdd
      val commercialProductRdd = validProductDf.select("provinceCode","originSource","carTypeBrandName","qualityType").rdd
      
      //将rdd拼接为(省份_数据来源_车型品牌_配件品质,1)
      //为减少后续计算量，将上述拼接的key前加上1-10随机数前缀
      //变为(randomNum?省份_数据来源_车型品牌_配件品质,1)的格式
      val prefixCommercialProductRdd = commercialProductRdd.map(d => {
        val prefix = scala.util.Random.nextInt(10)
        (prefix + "#" + d.getAs[String]("provinceCode") + "_" + d.getAs[String]("originSource") + "_" + d.getAs[String]("carTypeBrandName") + "_" + d.getAs[String]("qualityType"),1)
      })
      
      //对加上随机数后的rdd进行reduceByKey操作
      val partialCommercialProductCountWithPrefixRdd = prefixCommercialProductRdd.reduceByKey(_+_)
      
      //去除局部聚合后RDD的随机数前缀
      val partialCommercialProductCountRdd = partialCommercialProductCountWithPrefixRdd.map(tuple => (tuple._1.split("#")(1),tuple._2))
      
      //将去除随机数前缀的RDD进行全局聚合
      val commercialProductCountRdd = partialCommercialProductCountRdd
        .reduceByKey(_+_)
        .filter(product => product._1.split("_").length == 4)
        .foreachPartition(commercialProduct => {
          val list = new ListBuffer[Tuple5[String,String,String,String,Long]]
          commercialProduct.foreach(product => {
            val province = product._1.split("_")(0)  //省份
            val source = product._1.split("_")(1)  //数据来源
            val brand = product._1.split("_")(2)  //车型品牌
            val qualityType = product._1.split("_")(3)  //配件品质
            val productNum = product._2.toLong  //产品数
            list.append((province,source,brand,qualityType,productNum))
          })
           var conn: Connection = null
           var stmt: Statement = null
           try{
             conn = DriverManager.getConnection(url, user, password)
             stmt = conn.createStatement
             list.foreach(l => {
             val sql = (""
               + "INSERT INTO census_province_commercial_product(province,origin_source,brand_name,quality,no_products,create_time,update_time) "
               + "VALUES (" + l._1 + "," + l._2 + ",'" + l._3 + "','" + l._4 + "'," + l._5 + ",NOW(),NOW()) "
               + "ON DUPLICATE KEY UPDATE no_products = " + l._5 + ", update_time = NOW()")
  
               stmt.execute(sql)
            }) 
          
            }catch{
              case e: SQLException => e.printStackTrace
              case _: Exception => println("Exception happen when insert province census data")
            }finally{
               if(stmt != null){
                 stmt.close
               }
               if(conn != null){
                 conn.close
               }
             }
        })
    }
}