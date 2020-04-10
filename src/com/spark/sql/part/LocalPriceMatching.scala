package com.spark.sql.part

/**
 *		本地化价格比对 
 */

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import java.time.LocalDate
import org.apache.hadoop.io.compress.GzipCodec

class LocalPriceMatching(
  val sparkSession: SparkSession = null,
  val optionsMap: Map[String,String] = null,
  val dates: List[String] = null,
  val configs: Map[String,List[String]] = null,
  val target: String = "",
  val brands: String = "",
  val provinces: String = "") extends Serializable{
  
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
  private val targetProvince = target
  private val matchBrand = brands.split(",")
  private val matchProvince = provinces.split(",")
  options -= ("partitionColumn"); options -= ("lowerBound"); options -= ("upperBound"); options -= ("numPartitions")
  
  /**
   * 基本表名及标准品牌编码类
   */
  case class TableNameStandardCode(){
    /**
     * 获取目标省份对应的表名
     */
    def getTargetTableName = {
      val url = piccclaimDb; val user = piccclaimDbUser; val password = piccclaimDbPwd
      val tableName = "code_province_mapping"
      "picc_local_price_" + config.getProvinceName(url, user, password, tableName, targetProvince)
    }

    /**
     * 获取比对标准品牌名称list
     */
    def getMatchingBrandCode = {
      val url = piccclaimDb; val user = piccclaimDbUser; val password = piccclaimDbPwd
      val tableName = "jy_brand_manufacturer_mapping"
      val brandCodeList = new ListBuffer[String]
      for(brand <- matchBrand){
        val currBrandCode = config.getBrandCode(url, user, password, tableName, brand)
        brandCodeList.append(currBrandCode)
      }
      brandCodeList.toList
    }
    
    /**
     * 获取比对省份的表名
     */
    def getMatchingTableName = {
      val url = piccclaimDb; val user = piccclaimDbUser; val password = piccclaimDbPwd
      val tableName = "code_province_mapping"
      val matchingTableList = new ListBuffer[String]
      for(province <- matchProvince){
        val currProvince = config.getProvinceName(url, user, password, tableName, province)
        matchingTableList.append("local_price_" + currProvince)
      }
      matchingTableList.toList
    }
    
  }
  
  //所需品牌列表
  private val brandList = TableNameStandardCode().getMatchingBrandCode
  
  /**
   * 获取目标省份所需品牌本地化价格数据
   */
  private val targetProvinceBrandLocalPriceDf = {
    options += ("url" -> piccclaimDb); options += ("user" -> piccclaimDbUser); options += ("password" -> piccclaimDbPwd)
    options += ("dbtable" -> TableNameStandardCode().getTargetTableName)
    val partitionColumn = "id"
    options += ("partitionColumn" -> partitionColumn)
    options += ("lowerBound" -> "1")
    options += ("upperBound" -> config.getUpperBound(options,partitionColumn).toString)
    options += ("numPartitions" -> "50")
    val targetDf = spark.read.format("jdbc").options(options).load.na.drop(Array("standard_brand_code"))
      .filter(row => brandList.contains(row.getAs[String]("standard_brand_code")))
    targetDf
  }
  
  /**
   * 目标省份中所需品牌与交易数据比对
   */
  private def japOrderPriceMatching = {
    val localPriceDf = targetProvinceBrandLocalPriceDf
    options += ("dbtable" -> "jap_order_parts")
    val partitionColumn = "id"
    options += ("partitionColumn" -> partitionColumn)
    options += ("lowerBound" -> "1")
    options += ("upperBound" -> config.getUpperBound(options,partitionColumn).toString)
    options += ("numPartitions" -> "50")
    
    val japOrderDf = spark.read.format("jdbc").options(options).load.na.drop(Array("brand_code"))
      .filter(row => brandList.contains(row.getAs[String]("brand_code")))
    
    val japOrderOemDf = japOrderDf.filter(row => row.getAs[String]("quality").equals("1"))
    val japOrderAfmDf = japOrderDf.filter(row => row.getAs[String]("quality").equals("2"))
    
    val localPriceJapOrderTempDf1 = localPriceDf.as("df1").join(broadcast(japOrderOemDf).as("df2"), localPriceDf("standard_brand_code") === japOrderOemDf("brand_code") && localPriceDf("temp_oe") === japOrderOemDf("temp_oe"), "left")
      .select(
        col("df1.standard_brand_code"),
        col("df1.brand_name"),
        col("df1.part_name"),
        col("df1.part_remark"),
        col("df1.temp_oe"),
        col("df1.sys_manufacture_price"),
        col("df1.4s_price"),
        col("df1.market_oem_price"),
        col("df1.market_afm_price"),
        col("df2.sale_price") as "oem_sale_price",
        col("df2.sale_province") as "oem_sale_province")
    
    val configObj = new Config(piccclaimDb,piccclaimDbUser,piccclaimDbPwd)
    val getProvinceNameFunc = udf(configObj.getProvinceCHNNameByCode, StringType)  
    val localPriceJapOrderDf = localPriceJapOrderTempDf1.as("df1").join(broadcast(japOrderAfmDf).as("df2"), localPriceJapOrderTempDf1("standard_brand_code") === japOrderOemDf("brand_code") && localPriceDf("temp_oe") === japOrderOemDf("temp_oe"), "left")
      .select(
        col("df1.standard_brand_code"),
        col("df1.brand_name"),
        col("df1.part_name"),
        col("df1.part_remark"),
        col("df1.temp_oe"),
        col("df1.sys_manufacture_price"),
        col("df1.4s_price"),
        col("df1.market_oem_price"),
        col("df1.market_afm_price"),
        col("df1.oem_sale_price"),
        col("df1.oem_sale_province"),
        col("df2.sale_price") as "afm_sale_price",
        col("df2.sale_province") as "afm_sale_province")
        .withColumn("oem_sale_province_name", getProvinceNameFunc(col("oem_sale_province")))
        .withColumn("afm_sale_province_name", getProvinceNameFunc(col("afm_sale_province")))
        .drop(col("oem_sale_province")).drop(col("afm_sale_province"))
    localPriceJapOrderDf     
  }
  
  /**
   * 本地化价格与需求省份2019年12月本地化价格进行比对
   */
  private def matchingProvinceLocalPrice = {
    var japOrderOtherProvinceDf = japOrderPriceMatching
    val configObj = new Config(piccclaimDb,piccclaimDbUser,piccclaimDbPwd)
    for(table <- TableNameStandardCode().getMatchingTableName){
      options += ("dbtable" -> table)
      val partitionColumn = "id"
      options += ("partitionColumn" -> partitionColumn)
      options += ("lowerBound" -> "1")
      options += ("upperBound" -> config.getUpperBound(options,partitionColumn).toString)
      options += ("numPartitions" -> "50")
      val province = configObj.getProvinceCHNNameByName(table.split("_")(2))  //省份名称
      val otherLocalPriceDf = spark.read.format("jdbc").options(options).load.na.drop(Array("brand_code","oe"))
        .filter(row => brandList.contains(row.getAs[String]("brand_code")))
      japOrderOtherProvinceDf = japOrderOtherProvinceDf.as("df1").join(broadcast(otherLocalPriceDf).as("df2"), japOrderOtherProvinceDf("standard_brand_code") === otherLocalPriceDf("brand_code") && japOrderOtherProvinceDf("temp_oe") === otherLocalPriceDf("oe"),"left")
        .drop(col("df2.id")).drop(col("df2.id1")).drop(col("df2.province")).drop(col("df2.id2")).drop(col("df2.id3")).drop(col("df2.brand_code"))
        .drop(col("df2.brand_name")).drop(col("df2.part_name")).drop(col("df2.oe")).drop(col("df2.fours_price")).drop(col("df2.apply_price"))
        .withColumnRenamed("oem_price", province + "市场原厂价").withColumnRenamed("afm_price", province + "市场品牌价")
    }
    japOrderOtherProvinceDf = japOrderOtherProvinceDf
      .drop(col("standard_brand_code"))
      .withColumnRenamed("brand_name", "品牌").withColumnRenamed("part_name", "配件名称").withColumnRenamed("part_remark", "零件备注")
      .withColumnRenamed("temp_oe", "原厂零件编号").withColumnRenamed("sys_manufacture_price", "系统厂方价").withColumnRenamed("4s_price", "4S店价")
      .withColumnRenamed("market_oem_price", "市场原厂价").withColumnRenamed("market_afm_price", "市场品牌价").withColumnRenamed("oem_sale_price", "原厂件交易最低价")
      .withColumnRenamed("oem_sale_province_name", "原厂件交易最低价省份").withColumnRenamed("afm_sale_price", "品牌件交易最低价").withColumnRenamed("afm_sale_province_name", "品牌件交易最低价省份")
    japOrderOtherProvinceDf
  }
  
  /**
   * 返回价格比对
   */
  def priceMatching{
    val configObj = new Config(piccclaimDb,piccclaimDbUser,piccclaimDbPwd)
    val targetProvince = configObj.getProvinceCHNNameByCode(this.targetProvince)
    var matchProvince = ""
    for(ele <- this.matchProvince) 
      matchProvince += configObj.getProvinceCHNNameByCode(ele) + "_"
    val resultDf = matchingProvinceLocalPrice
      
    for(ele <- this.matchBrand){
      val brandResultDf = resultDf.filter(row => row.getAs[String]("品牌").equals(ele))
      val fileName = targetProvince + "---" + matchProvince + ele + "_" + LocalDate.now() + ".csv"
      brandResultDf.coalesce(20).write.option("header", true).csv("hdfs://master:9000/local_price_match/"+fileName)
      
    }
        
  }
}