package com.spark.sql.part

import scala.collection.mutable.Map
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer
import java.sql.{Connection,DriverManager,Statement,SQLException,Date}
import java.text.SimpleDateFormat

class JapInquiryOrderMinPrice(
  val sparkSession: SparkSession = null,
  val optionsMap: Map[String,String] = null,
  val dates: List[String] = null,
  val configs: Map[String,List[String]] = null
  )extends Serializable{
  
  //初始化变量
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
  
  //报价数据类
  case class MinOfferPrice(pid:String, province:String, brand:String, brandCode: String, oe:String, tempOe:String, quality:String, salePrice:java.math.BigDecimal,deliverTime:String)
  
  /**
   * 获取交易数据
   */
  private def getOrderMinPrice{
    var partitionColumn = "damage_parts_id"
    
    options += ("url" -> jiaanpeiReportDb)    
    options += ("user" -> jiaanpeiReportDbUser)
    options += ("password" -> jiaanpeiReportDbPwd)
    options += ("dbtable" -> "base_jap_parts")
    options += ("partitionColumn" -> partitionColumn)
    options += ("lowerBound" -> "1")
    options += ("upperBound" -> config.getUpperBound(options,partitionColumn).toString)
    options += ("numPartitions" -> "50")
    
    val baseInquiryDataDf = spark.read.format("jdbc").options(options)
      .load.na.drop(Array("temp_oe","car_brand","quality_type","part_oe","deliver_time","sale_price"))
      .filter(row => {
        val deliverDate: String = row.getAs("deliver_time").toString.split(" ")(0)
        val oe: String = row.getAs("temp_oe"); val brand: String = row.getAs("car_brand")
        if(!deliverDate.equals(date)) false
        else if(oe.equals("") || oe.startsWith("JY")) false  //剔除无OE或OE为JY开头的数据
        else if(brand.equals("")) false  //剔除无品牌数据
        else if(row.getAs[java.math.BigDecimal]("sale_price").doubleValue <= 5) false  //剔除配件价格<=5的数据
        else true
      })
         
    var orderMinPriceDf = baseInquiryDataDf.groupBy("car_brand", "temp_oe", "quality_type")
      .agg(
          min(col("sale_price")) as "minSalePrice")
      .withColumn("iPid", concat(col("car_brand"),col("temp_oe"),col("quality_type")))
      .withColumn("updateTime", current_timestamp())
    
    orderMinPriceDf = baseInquiryDataDf.as("df1").join(broadcast(orderMinPriceDf).as("df2"), 
        baseInquiryDataDf("car_brand") === baseInquiryDataDf("car_brand") && baseInquiryDataDf("temp_oe") === orderMinPriceDf("temp_oe") && baseInquiryDataDf("quality_type") === orderMinPriceDf("quality_type") && baseInquiryDataDf("sale_price") === orderMinPriceDf("minSalePrice"),
        "inner")
      .select(
          col("df2.iPid"),col("df2.car_brand"),col("df2.temp_oe"),col("df2.quality_type"),col("df2.minSalePrice"),
          col("df1.province"),col("df1.part_oe") as "oe",col("df1.deliver_time") as "deliverTime")
    
    /**
     * 关联原数据，更新成交金额
     */
    partitionColumn = "id"
    options += ("url" -> piccclaimDb)
    options += ("user" -> piccclaimDbUser)
    options += ("password" -> piccclaimDbPwd)
    options += ("dbtable" -> "jap_order_parts")
    options += ("partitionColumn" -> partitionColumn)
    options += ("lowerBound" -> "1")
    options += ("upperBound" -> config.getUpperBound(options,partitionColumn).toString)
    options += ("numPartitions" -> "50")
    val todayOfLastYear = config.getDateValue(options, date, "todayOfLastYear")
    val japOrderPartsDf = spark.read.format("jdbc").options(options).load.na.drop(Array("deliver_time"))
    val selectedJapOrderPartsTempDf = japOrderPartsDf.as("df1").join(broadcast(orderMinPriceDf).as("df2"), japOrderPartsDf("pid") === orderMinPriceDf("iPid"),"right")
      .select(
          col("df2.iPid") as "pid",
          when(col("df1.sale_province").isNull, col("df2.province"))
            .otherwise(when(col("df1.sale_price") > col("df2.minSalePrice") || col("df1.deliver_time") < todayOfLastYear,col("df2.province")).otherwise(col("df1.sale_province"))) as "province",
          col("df2.car_brand"),
          col("df2.quality_type"),
          col("df2.oe"),
          col("df2.temp_oe"),
          when(col("df1.sale_price").isNull,col("df2.minSalePrice"))
            .otherwise(when(col("df1.sale_price") > col("df2.minSalePrice") || col("df1.deliver_time") < todayOfLastYear,col("df2.minSalePrice")).otherwise(col("df1.sale_price"))) as "salePrice",
          when(col("df1.deliver_time").isNull,col("df2.deliverTime"))
            .otherwise(when(col("df1.sale_price") > col("df2.minSalePrice") || col("df1.deliver_time") < todayOfLastYear,col("df2.deliverTime")).otherwise(col("df1.deliver_time"))) as "deliverTime"
      )    
      
    options += ("dbtable" -> "jy_brand_manufacturer_mapping")
    options -= ("partitionColumn"); options -= ("lowerBound"); options -= ("upperBound"); options -= ("numPartitions")
    val brandCodeDf = spark.read.format("jdbc").options(options).load
    val selectedJapOrderPartsDf = selectedJapOrderPartsTempDf.as("df1").join(broadcast(brandCodeDf).as("df2"), selectedJapOrderPartsTempDf("car_brand") === brandCodeDf("standard_brand_name"),"left")
      
    /**
     * 插入更新成交数据  
     */
    val url = piccclaimDb; val user = piccclaimDbUser; val password = piccclaimDbPwd; val tableName = "jap_order_parts"
    selectedJapOrderPartsDf.foreachPartition(iter => {
      val list = new ListBuffer[MinOfferPrice]
      iter.foreach(row => {
        val pid = row.getAs[String]("pid")
        val province = row.getAs[String]("province")
        val brand = row.getAs[String]("car_brand")
        val brandCode = row.getAs[String]("standard_brand_code")
        val oe = row.getAs[String]("oe")
        val tempOe = row.getAs[String]("temp_oe")
        val quality = row.getAs[String]("quality_type")
        val minPrice = row.getAs[java.math.BigDecimal]("salePrice")
        val deliverTime = row.getAs[Timestamp]("deliverTime").toString.split(" ")(0)
        list.append(MinOfferPrice(pid,province,brand,brandCode,oe,tempOe,quality,minPrice,deliverTime))
      })
      var conn: Connection = null
      var stmt: Statement = null
      try{
        conn = DriverManager.getConnection(url,user,password)
        stmt = conn.createStatement
        var sql = ""
        for(ele <- list){
          sql = (""
              + "INSERT INTO " + tableName + " (pid,sale_province,brand_code,brand,oe,temp_oe,quality,sale_price,deliver_time,create_time,update_time) "
              + "VALUES ('" + ele.pid + "','" + ele.province + "','" + ele.brandCode + "','" + ele.brand + "','" + ele.oe + "','" + ele.tempOe + "','" + ele.quality + "',"
              + ele.salePrice + ",'" + ele.deliverTime + "',NOW(),NOW()) "
              + "ON DUPLICATE KEY UPDATE "
                + "sale_province = '" + ele.province + "',"
                + "sale_price = " + ele.salePrice + ","
                + "deliver_time = '" + ele.deliverTime + "',"
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
  
  //计算最小值
  def calculatePartMinPrice{
    getOrderMinPrice
  }
}