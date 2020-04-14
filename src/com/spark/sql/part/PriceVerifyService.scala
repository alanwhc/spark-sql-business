package com.spark.sql.part

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.{Map,ListBuffer}
import org.apache.spark.sql.functions._
import java.sql.{Connection,DriverManager,Statement,SQLException,Date,Timestamp}
import java.text.SimpleDateFormat

class PriceVerifyService(
  val sparkSession: SparkSession = null,
  val optionsMap: Map[String,String] = null,
  val dates: List[String] = null,
  val configs: Map[String,List[String]] = null) extends Serializable {
  
  private val spark = sparkSession
  private val options = optionsMap
  private val date = dates(0)  
  private val year = dates(1)
  private val month = dates(2)
  private val day = dates(3)
  private val jiaanpeiReportDb = configs("url")(0)
  private val piccclaimDb = configs("url")(1)
  private val jiaanpeiReportDbUser = configs("user")(0)
  private val piccclaimDbUser = configs("user")(1)
  private val jiaanpeiReportDbPwd = configs("password")(0)
  private val piccclaimDbPwd = configs("password")(1)
  private val config = new Config()
  options -= ("partitionColumn"); options -= ("lowerBound"); options -= ("upperBound"); options -= ("numPartitions")
  
  case class MinSalePrice(pid: String = "", brand: String = "", brandCode: String = "", oe: String = "", tempOe: String = "", quality: String = "", 
      minSalePrice: java.math.BigDecimal = null, minSaleProvince: String = "", minSaleDeliverTime: String = "")
  case class MaxSalePrice(pid: String = "", brand: String = "", brandCode: String = "", oe: String = "", tempOe: String = "", quality: String = "", 
      maxSalePrice: java.math.BigDecimal = null, maxSaleProvince: String = "", maxSaleDeliverTime: String = "")
  case class AverageSalePrice(pid: String = "", brand: String = "", brandCode: String = "", oe: String = "", tempOe: String = "", quality: String = "", 
      avgSalePrice: java.math.BigDecimal = null, noSaleParts: Long = 0)
  case class BaseOrderPart(orderItemId: Long = 0, brand: String = "", brandCode: String = "", oe: String = "", tempOe: String = "",
      quality: String = "", partName: String = "", province: String = "",manuPrice: java.math.BigDecimal = null, salePrice: java.math.BigDecimal = null, deliverTime: String = "")
    
  def calculatePrice{
    var partitionColumn = "damage_parts_id"
    
    options += ("url" -> jiaanpeiReportDb)    
    options += ("user" -> jiaanpeiReportDbUser)
    options += ("password" -> jiaanpeiReportDbPwd)
    options += ("dbtable" -> "base_jap_parts")
    options += ("partitionColumn" -> partitionColumn)
    options += ("lowerBound" -> "1")
    options += ("upperBound" -> config.getUpperBound(options,partitionColumn).toString)
    options += ("numPartitions" -> "50")
    
    val baseOrderDataDf = spark.read.format("jdbc").options(options)
      .load.na.drop(Array("temp_oe","car_brand","quality_type","part_oe","deliver_time","sale_price"))
      .filter(row => {
        val deliverDate: String = row.getAs("deliver_time").toString.split(" ")(0)
        val oe: String = row.getAs("temp_oe"); val brand: String = row.getAs("car_brand")
        if(!deliverDate.equals(date)) false
        /*if(new SimpleDateFormat("yyyy-MM-dd").parse(deliverDate).getTime > new SimpleDateFormat("yyyy-MM-dd").parse(date).getTime
          || new SimpleDateFormat("yyyy-MM-dd").parse(deliverDate).getTime < new SimpleDateFormat("yyyy-MM-dd").parse("2020-01-14").getTime) false*/
        else if(oe.equals("") || oe.startsWith("JY")) false  //剔除无OE或OE为JY开头的数据
        else if(brand.equals("")) false  //剔除无品牌数据
        else if(row.getAs[java.math.BigDecimal]("sale_price").doubleValue <= 5) false  //剔除配件价格<=5的数据
        else true
      })
            
    var orderMinPriceDf = baseOrderDataDf.groupBy("car_brand", "temp_oe", "quality_type")
      .agg(
          min(col("sale_price")) as "minSalePrice")
      .withColumn("pid", concat(col("car_brand"),col("temp_oe"),col("quality_type")))
    orderMinPriceDf = baseOrderDataDf.as("df1")
      .join(broadcast(orderMinPriceDf).as("df2"), 
            baseOrderDataDf("car_brand") === orderMinPriceDf("car_brand") && baseOrderDataDf("temp_oe") === orderMinPriceDf("temp_oe") && baseOrderDataDf("quality_type") === orderMinPriceDf("quality_type") && baseOrderDataDf("sale_price") === orderMinPriceDf("minSalePrice"),
            "inner")
      .select(col("df2.pid"), col("df2.car_brand"),col("df2.temp_oe"),col("df2.quality_type"),col("df2.minSalePrice"),
        col("df1.province"),col("df1.deliver_time") as "deliverTime",col("df1.part_oe") as "oe")  

    var orderMaxPriceDf = baseOrderDataDf.groupBy("car_brand", "temp_oe", "quality_type")
      .agg(
          max(col("sale_price")) as "maxSalePrice")
      .withColumn("pid", concat(col("car_brand"),col("temp_oe"),col("quality_type")))
    orderMaxPriceDf = baseOrderDataDf.as("df1")
      .join(broadcast(orderMaxPriceDf).as("df2"), 
            baseOrderDataDf("car_brand") === orderMaxPriceDf("car_brand") && baseOrderDataDf("temp_oe") === orderMaxPriceDf("temp_oe") && baseOrderDataDf("quality_type") === orderMaxPriceDf("quality_type") && baseOrderDataDf("sale_price") === orderMaxPriceDf("maxSalePrice"),
            "inner")
      .select(col("df2.pid"), col("df2.car_brand"),col("df2.temp_oe"),col("df2.quality_type"),col("df2.maxSalePrice"),
        col("df1.province"),col("df1.deliver_time") as "deliverTime",col("df1.part_oe") as "oe") 
      
    val avgPriceDf = baseOrderDataDf.groupBy("car_brand", "temp_oe", "quality_type")
      .agg(
          min(col("part_oe")) as "oe",
          avg(col("sale_price")) as "averageSalePrice",
          count(col(partitionColumn)) as "noSaleParts")
      .withColumn("pid", concat(col("car_brand"),col("temp_oe"),col("quality_type")))
      
    /**
     * 关联原数据，更新价格
     */
    partitionColumn = "id"
    options += ("url" -> piccclaimDb); options += ("user" -> piccclaimDbUser); options += ("password" -> piccclaimDbPwd)
    options += ("dbtable" -> "jap_order_parts_complex")
    options += ("partitionColumn" -> partitionColumn);options += ("lowerBound" -> "1");options += ("upperBound" -> config.getUpperBound(options,partitionColumn).toString);options += ("numPartitions" -> "50")
    val ninetyDaysBefore = config.getDateValue(options, date, "dayOfLastQuarter")
    val japOrderPartsDf = spark.read.format("jdbc").options(options).load
    
    //更新最低价
    val tempDataDf1 = japOrderPartsDf.as("df1").join(broadcast(orderMinPriceDf).as("df2"), japOrderPartsDf("pid") === orderMinPriceDf("pid"), "right")
      .select(col("df2.pid"),
        when(col("df1.min_sale_province").isNull,col("df2.province"))
          .otherwise(when(col("df1.min_sale_price") > col("df2.minSalePrice") || col("df1.min_sale_deliver_time") < ninetyDaysBefore,col("df2.province")).otherwise(col("df1.min_sale_province"))) as "minSaleProvince",
        col("df2.car_brand"),
        col("df2.quality_type"),
        col("df2.oe"),
        col("df2.temp_oe"),
        when(col("df1.min_sale_price").isNull,col("df2.minSalePrice"))
          .otherwise(when(col("df1.min_sale_price") > col("df2.minSalePrice") || col("df1.min_sale_deliver_time") < ninetyDaysBefore,col("df2.minSalePrice")).otherwise(col("df1.min_sale_price"))) as "minSalePrice",
        when(col("df1.min_sale_deliver_time").isNull,col("df2.deliverTime"))
          .otherwise(when(col("df1.min_sale_price") > col("df2.minSalePrice") || col("df1.min_sale_deliver_time") < ninetyDaysBefore,col("df2.deliverTime")).otherwise(col("df1.min_sale_deliver_time"))) as "minSaleDeliverTime"
      )
      
    //更新最高价
    val tempDataDf2 = japOrderPartsDf.as("df1").join(broadcast(orderMaxPriceDf).as("df2"), japOrderPartsDf("pid") === orderMaxPriceDf("pid"), "right")
      .select(col("df2.pid"),
        when(col("df1.max_sale_province").isNull,col("df2.province"))
          .otherwise(when(col("df1.max_sale_price") <= col("df2.maxSalePrice") || col("df1.max_sale_deliver_time") < ninetyDaysBefore,col("df2.province")).otherwise(col("df1.max_sale_province"))) as "maxSaleProvince",
        col("df2.car_brand"),
        col("df2.quality_type"),
        col("df2.oe"),
        col("df2.temp_oe"),
        when(col("df1.max_sale_price").isNull,col("df2.maxSalePrice"))
          .otherwise(when(col("df1.max_sale_price") <= col("df2.maxSalePrice") || col("df1.max_sale_deliver_time") < ninetyDaysBefore,col("df2.maxSalePrice")).otherwise(col("df1.max_sale_price"))) as "maxSalePrice",
        when(col("df1.max_sale_deliver_time").isNull,col("df2.deliverTime"))
          .otherwise(when(col("df1.max_sale_price") <= col("df2.maxSalePrice") || col("df1.max_sale_deliver_time") < ninetyDaysBefore,col("df2.deliverTime")).otherwise(col("df1.max_sale_deliver_time"))) as "maxSaleDeliverTime"
      )
    
    //平均价计算
    val tempDataDf3 = japOrderPartsDf.as("df1").join(broadcast(avgPriceDf).as("df2"),japOrderPartsDf("pid") === avgPriceDf("pid"),"right")
      .select(col("df2.pid"),
        col("df2.car_brand"),
        col("df2.quality_type"),
        col("df2.oe"),
        col("df2.temp_oe"),
        when(col("df1.avg_sale_price").isNull,col("df2.averageSalePrice"))
          .otherwise(round((col("df1.avg_sale_price") * col("df1.sale_times") + col("df2.averageSalePrice") * col("df2.noSaleParts")) / (col("df1.sale_times") + col("df2.noSaleParts")),2)) as "avgSalePrice",
        when(col("df1.sale_times").isNull,col("df2.noSaleParts"))
          .otherwise(col("df1.sale_times") + col("df2.noSaleParts")) as "noSaleParts"
        )
        
    //匹配标准品牌名称
    options += ("dbtable" -> "jy_brand_manufacturer_mapping")
    options -= ("partitionColumn"); options -= ("lowerBound"); options -= ("upperBound"); options -= ("numPartitions")
    val brandCodeDf = spark.read.format("jdbc").options(options).load
    val minOrderPriceDf = tempDataDf1.as("df1").join(broadcast(brandCodeDf).as("df2"), tempDataDf1("car_brand") === brandCodeDf("standard_brand_name"), "left")
    val maxOrderPriceDf = tempDataDf2.as("df1").join(broadcast(brandCodeDf).as("df2"), tempDataDf2("car_brand") === brandCodeDf("standard_brand_name"), "left")
    val avgOrderPriceDf = tempDataDf3.as("df1").join(broadcast(brandCodeDf).as("df2"), tempDataDf3("car_brand") === brandCodeDf("standard_brand_name"), "left")
    var baseOrderDataCodeDf = baseOrderDataDf.as("df1").join(broadcast(brandCodeDf).as("df2"), tempDataDf3("car_brand") === brandCodeDf("standard_brand_name"), "left")
    options += ("dbtable" -> "jy_parts_standard")
    val sysPriceDf = spark.read.format("jdbc").options(options).load
    baseOrderDataCodeDf = baseOrderDataCodeDf.join(sysPriceDf,Seq("standard_brand_code","temp_oe"),"left")
    
    options += ("dbtable" -> "jap_order_item")
    baseOrderDataCodeDf.foreachPartition(iter => {
      val list = new ListBuffer[BaseOrderPart]
      iter.foreach(row => {
        val orderItemId: Long = row.getAs("orderitem_id");val brandCode: String = row.getAs("standard_brand_code"); val brand: String = row.getAs("car_brand")
        val oe: String = row.getAs("part_oe"); val tempOe: String = row.getAs("temp_oe");val quality: String = row.getAs("quality_type");
        val partName: String = row.getAs("part_name"); val province: String = row.getAs("province"); val manuPrice: java.math.BigDecimal = row.getAs("sys_manufacture_price")
        val salePrice: java.math.BigDecimal = row.getAs("sale_price")
        val deliverTime = row.getAs[Timestamp]("deliver_time").toString.split(" ")(0)
        list.append(BaseOrderPart(orderItemId,brand,brandCode,oe,tempOe,quality,partName,province,manuPrice,salePrice,deliverTime))
      })
      var conn: Connection = null
      var stmt: Statement = null
      try{
        conn = DriverManager.getConnection(piccclaimDb,piccclaimDbUser,piccclaimDbPwd)
        stmt = conn.createStatement
        var sql = ""
        for(ele <- list){
          sql = (""
              + "INSERT INTO " + options("dbtable") + " (orderitem_id,brand_code,brand,part_oe,temp_oe,quality,part_name,province,manufacture_price,sale_price,deliver_time,create_time,update_time) "
              + "VALUES (" + ele.orderItemId + ",'" + ele.brandCode + "','" + ele.brand + "','" + ele.oe + "','" + ele.tempOe + "','" + ele.quality + "','"
              + ele.partName + "','" + ele.province + "'," + ele.manuPrice + "," + ele.salePrice + ",'" + ele.deliverTime + "',NOW(),NOW()) "
              + "ON DUPLICATE KEY UPDATE "
                + "update_time = NOW();")
          stmt.executeUpdate(sql)
        }
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
    })
    
    options += ("dbtable" -> "jap_order_parts_complex")
    //将最低价插入更新
    minOrderPriceDf.foreachPartition(iter =>{
      val list = new ListBuffer[MinSalePrice]
      iter.foreach(row => {
        val pid = row.getAs[String]("pid")
        val province = row.getAs[String]("minSaleProvince")
        val brand = row.getAs[String]("car_brand")
        val brandCode = row.getAs[String]("standard_brand_code")
        val oe = row.getAs[String]("oe")
        val tempOe = row.getAs[String]("temp_oe")
        val quality = row.getAs[String]("quality_type")
        val minPrice = row.getAs[java.math.BigDecimal]("minSalePrice")
        val deliverTime = row.getAs[Timestamp]("minSaleDeliverTime").toString.split(" ")(0)
        list.append(MinSalePrice(pid,brand,brandCode,oe,tempOe,quality,minPrice,province,deliverTime))
      })
      var conn: Connection = null
      var stmt: Statement = null
      try{
        conn = DriverManager.getConnection(piccclaimDb,piccclaimDbUser,piccclaimDbPwd)
        stmt = conn.createStatement
        var sql = ""
        for(ele <- list){
          sql = (""
              + "INSERT INTO " + options("dbtable") + " (pid,brand_code,brand,oe,temp_oe,quality,min_sale_price,min_sale_province,min_sale_deliver_time,create_time,update_time) "
              + "VALUES ('" + ele.pid + "','" + ele.brandCode + "','" + ele.brand + "','" + ele.oe + "','" + ele.tempOe + "','" + ele.quality + "',"
              + ele.minSalePrice + ",'" + ele.minSaleProvince + "','" + ele.minSaleDeliverTime + "',NOW(),NOW()) "
              + "ON DUPLICATE KEY UPDATE "
                + "min_sale_province = '" + ele.minSaleProvince + "',"
                + "min_sale_price = " + ele.minSalePrice + ","
                + "min_sale_deliver_time = '" + ele.minSaleDeliverTime + "',"
                + "update_time = NOW();")
          stmt.executeUpdate(sql)
        }
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
    })
    
    //将最高价插入更新
    maxOrderPriceDf.foreachPartition(iter =>{
      val list = new ListBuffer[MaxSalePrice]
      iter.foreach(row => {
        val pid = row.getAs[String]("pid")
        val province = row.getAs[String]("maxSaleProvince")
        val brand = row.getAs[String]("car_brand")
        val brandCode = row.getAs[String]("standard_brand_code")
        val oe = row.getAs[String]("oe")
        val tempOe = row.getAs[String]("temp_oe")
        val quality = row.getAs[String]("quality_type")
        val maxPrice = row.getAs[java.math.BigDecimal]("maxSalePrice")
        val deliverTime = row.getAs[Timestamp]("maxSaleDeliverTime").toString.split(" ")(0)
        list.append(MaxSalePrice(pid,brand,brandCode,oe,tempOe,quality,maxPrice,province,deliverTime))
      })
      var conn: Connection = null
      var stmt: Statement = null
      try{
        conn = DriverManager.getConnection(piccclaimDb,piccclaimDbUser,piccclaimDbPwd)
        stmt = conn.createStatement
        var sql = ""
        for(ele <- list){
          sql = (""
              + "INSERT INTO " + options("dbtable") + " (pid,brand_code,brand,oe,temp_oe,quality,max_sale_price,max_sale_province,max_sale_deliver_time,create_time,update_time) "
              + "VALUES ('" + ele.pid + "','" + ele.brandCode + "','" + ele.brand + "','" + ele.oe + "','" + ele.tempOe + "','" + ele.quality + "',"
              + ele.maxSalePrice + ",'" + ele.maxSaleProvince + "','" + ele.maxSaleDeliverTime + "',NOW(),NOW()) "
              + "ON DUPLICATE KEY UPDATE "
                + "max_sale_province = '" + ele.maxSaleProvince + "',"
                + "max_sale_price = " + ele.maxSalePrice + ","
                + "max_sale_deliver_time = '" + ele.maxSaleDeliverTime + "',"
                + "update_time = NOW();")
          stmt.executeUpdate(sql)
        }
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
    })
    
    //将平均价插入更新
    avgOrderPriceDf.foreachPartition(iter =>{
      val list = new ListBuffer[AverageSalePrice]
      iter.foreach(row => {
        val pid = row.getAs[String]("pid")
        val brand = row.getAs[String]("car_brand")
        val brandCode = row.getAs[String]("standard_brand_code")
        val oe = row.getAs[String]("oe")
        val tempOe = row.getAs[String]("temp_oe")
        val quality = row.getAs[String]("quality_type")
        val price = row.getAs[java.math.BigDecimal]("avgSalePrice")
        val noParts = row.getAs[Long]("noSaleParts")
        list.append(AverageSalePrice(pid,brand,brandCode,oe,tempOe,quality,price,noParts))
      })
      var conn: Connection = null
      var stmt: Statement = null
      try{
        conn = DriverManager.getConnection(piccclaimDb,piccclaimDbUser,piccclaimDbPwd)
        stmt = conn.createStatement
        var sql = ""
        for(ele <- list){
          sql = (""
              + "INSERT INTO " + options("dbtable") + " (pid,brand_code,brand,oe,temp_oe,quality,avg_sale_price,sale_times,create_time,update_time) "
              + "VALUES ('" + ele.pid + "','" + ele.brandCode + "','" + ele.brand + "','" + ele.oe + "','" + ele.tempOe + "','" + ele.quality + "',"
              + ele.avgSalePrice + ",'" + ele.noSaleParts + "',NOW(),NOW()) "
              + "ON DUPLICATE KEY UPDATE "
                + "avg_sale_price = " + ele.avgSalePrice + ","
                + "sale_times = " + ele.noSaleParts + ","
                + "update_time = NOW();")
          stmt.executeUpdate(sql)
        }
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
    })
  }
}