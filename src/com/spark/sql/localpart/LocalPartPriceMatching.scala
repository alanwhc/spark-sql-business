package com.spark.sql.localpart

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.{Map,ListBuffer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import java.sql.{Connection,Statement,DriverManager,SQLException,ResultSet,Date}

class LocalPartPriceMatching(
  val sparkSession: SparkSession = null,
  val optionsMap: Map[String,String] = null,
  val dates: List[String] = null,
  val target: String = "",
  val provinces: String = "") extends Serializable{
  
  //初始化私有变量
  private val spark = sparkSession
  private val options = optionsMap
  private val date = dates(0); private val year = dates(1); private val month = dates(2); private val day = dates(3)
  private val config = new Configuration(options("url"),options("user"),options("password"))
  private val parameterObj = new ParameterTranslation(config)
  //获取相关基础信息
  private val targetProvinceTableName = parameterObj.getTargetProvinceTableName(target, "GetProvincePinYin")
  private val matchProvinceTableName = parameterObj.getMatchingProvinceTableName(provinces.split(","), "GetProvincePinYin")
  private val brandCodeList = config.getBrandList("local_price_insert_multiclaim_match")
  
  case class JapMin(id: Long, oemPrice: java.math.BigDecimal, oemProvince: String, afmPrice: java.math.BigDecimal, afmProvince: String)

  //获取目标省份所需本地化价格数据
  private val targetProvinceLocalPriceDf = {
    options += ("dbtable" -> "local_price_insert_multiclaim_match")
    options += ("partitionColumn" -> "id");options += ("lowerBound" -> "1")
    options += ("upperBound" -> config.getUpperBound(options("dbtable"),"id").toString);options += ("numPartitions" -> "50")
    val replaceSpecialCharacterFunc = udf(parameterObj.removeSpecialCharacter, StringType)
    spark.read.format("jdbc").options(options).load
      .withColumn("temp_oe", replaceSpecialCharacterFunc(col("oe")))
  }
  
  //与交易数据进行比对
  private val japOrderPriceMatchingDf = {
    options += ("dbtable" -> "jap_order_parts")
    options += ("partitionColumn" -> "id");options += ("lowerBound" -> "1")
    options += ("upperBound" -> config.getUpperBound(options("dbtable"),"id").toString);options += ("numPartitions" -> "50")
    
    val japOrderOemDf = spark.read.format("jdbc").options(options).load.filter(row => row.getAs[String]("quality").equals("1"))
    val japOrderAfmDf = spark.read.format("jdbc").options(options).load.filter(row => row.getAs[String]("quality").equals("2"))
    
    val getProvinceChnNameFunc = udf(config.getProvinceCHNNameByCode, StringType)
    var localPriceMinJapOrderDf = targetProvinceLocalPriceDf.as("df1")
      .join(broadcast(japOrderOemDf).as("df2"),
        targetProvinceLocalPriceDf("standard_brand_code") === japOrderOemDf("brand_code") && targetProvinceLocalPriceDf("temp_oe") === japOrderOemDf("temp_oe"),
        "left")
      .withColumnRenamed("sale_price", "oem_sale_price").withColumnRenamed("sale_province", "oem_sale_province")
      .drop(col("df2.brand_code")).drop(col("df2.temp_oe")).drop(col("df2.oe")).drop(col("df2.id")).drop(col("df2.pid"))
      .drop(col("df2.quality")).drop(col("df2.brand")).drop(col("df2.deliver_time")).drop(col("df2.create_time")).drop(col("df2.update_time"))
      .withColumn("oem_sale_province_name", getProvinceChnNameFunc(col("oem_sale_province")))
      
    localPriceMinJapOrderDf = localPriceMinJapOrderDf.as("df1")
      .join(broadcast(japOrderAfmDf).as("df2"),
        localPriceMinJapOrderDf("standard_brand_code") === japOrderAfmDf("brand_code") && localPriceMinJapOrderDf("temp_oe") === japOrderAfmDf("temp_oe"),
        "left")
      .withColumnRenamed("sale_price", "afm_sale_price").withColumnRenamed("sale_province", "afm_sale_province")
      .withColumn("afm_sale_province_name", getProvinceChnNameFunc(col("afm_sale_province")))
      .drop(col("df2.brand_code")).drop(col("df2.temp_oe")).drop(col("df2.oe")).drop(col("df2.id")).drop(col("df2.pid"))
      .drop(col("df2.quality")).drop(col("df2.brand")).drop(col("df2.deliver_time")).drop(col("df2.create_time")).drop(col("df2.update_time"))
   
    localPriceMinJapOrderDf.select(col("id"), col("oem_sale_price"), col("oem_sale_province_name"), col("afm_sale_price"), col("afm_sale_province_name"))
  }
  
  /**
   * 添加或删除相应的列
   */
  def addNewColumn{
    val tableName = "local_price_insert_multiclaim_match"
    //获取需要添加的列List
    val columnList = new ListBuffer[String]
    //添加对比省份的列
    for(prov <- provinces.split(",")){
      columnList.append(config.getBrandCodeProvinceName("code_province_mapping", prov, "GetProvincePinYin") + "_oem_price")
      columnList.append(config.getBrandCodeProvinceName("code_province_mapping", prov, "GetProvincePinYin") + "_afm_price")     
    }
    var previousColumn = "jap_min_afm_province"
    for(ele <- columnList.toList){
      var dateType = ""; var defaultValue = ""
      if(ele.endsWith("price")){
        dateType = "DECIMAL(11,2)"; defaultValue = "0"
      }else{
        dateType = "VARCHAR(30)"; defaultValue = ""
      }
      config.dropColumn(tableName, ele)
      config.addColumn(tableName, ele, dateType, defaultValue, "", previousColumn)
      previousColumn = ele
    }
  }
  
  def priceMatching{
    //更新历史交易最低价及省份
    japOrderPriceMatchingDf.na.fill(0, Array("oem_sale_price","afm_sale_price"))
      .filter(row => row.getAs[java.math.BigDecimal]("oem_sale_price").doubleValue != 0 || row.getAs[java.math.BigDecimal]("afm_sale_price").doubleValue != 0)
      .foreachPartition(iter => {
        val list = new ListBuffer[JapMin]
        iter.foreach(row => {
          val id: Long = row.getAs("id"); val oemPrice: java.math.BigDecimal = row.getAs("oem_sale_price"); val oemProvince: String = row.getAs("oem_sale_province_name")
          val afmPrice: java.math.BigDecimal = row.getAs("afm_sale_price"); val afmProvince: String = row.getAs("afm_sale_province_name")
          list.append(JapMin(id,oemPrice,oemProvince,afmPrice,afmProvince))
        })
        var conn: Connection = null
        var stmt: Statement = null
        try{
          conn = DriverManager.getConnection(options("url"), options("user"), options("password"))
          stmt = conn.createStatement
          for(ele <- list){
            val sql = (""
              + "UPDATE local_price_insert_multiclaim_match " 
              + "SET "
                + "jap_min_oem_price = " + ele.oemPrice + ","
                + "jap_min_oem_province = '" + ele.oemProvince + "',"
                + "jap_min_afm_price = " + ele.afmPrice + ","
                + "jap_min_afm_province = '" + ele.afmProvince + "',"
                + "update_time = NOW() "
              + "WHERE id = " + ele.id + ";")
            stmt.execute(sql)
          } 
        }catch{
          case e: SQLException => e.printStackTrace
        }finally{
          if(stmt != null)
            stmt.close
          if(conn != null)
            conn.close
        }
      })
    //config.addColumn("local_price_insert_multiclaim_match", "oem_price", "DECIMAL(11,2)", "0", "原厂件交易最低价", "reman_price_remark")
    //config.addColumn("local_price_insert_multiclaim_match", "oem_price_province", "VARCHAR(20)", "", "原厂件交易省份", "oem_price")
  }
}