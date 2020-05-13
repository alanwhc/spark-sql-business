package com.spark.sql.part.alias

/**
 * 询价单配件
 */
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import scala.collection.mutable.ListBuffer
import java.sql.{Connection,Statement,DriverManager,SQLException}
import java.text.SimpleDateFormat

class InquiryPart(
  val sparkSession: SparkSession = null,
  val optionsMap: Map[String,String] = null,
  val dates: List[String] = null,
  val configs: Map[String,List[String]] = null
  ) extends Serializable{
  
  /**
   * 初始化变量
   */
  private val spark = sparkSession
  private val options = optionsMap
  private val date = dates(0)  
  private val year = dates(1)
  private val month = dates(2)
  private val day = dates(3)
  private val jiaanpeiDb = configs("url")(0)
  private val jiaanpeiDbUser = configs("user")(0)
  private val jiaanpeiDbPwd = configs("password")(0)
  private val partDb = configs("url")(1)
  private val partDbUser = configs("user")(1)
  private val partDbPwd = configs("password")(1)
  private val productDb = configs("url")(2)
  private val productDbUser = configs("user")(2)
  private val productDbPwd = configs("password")(2)
  private var config = new Config()
  
  /**
   * 询价配件类
   */
  case class InquiryPartInfo(partId: Long = 0, standardPartName: String = "", partName: String = "", partOe: String = "", vehicleType: String = "")
  case class OfferPartBrand(id: Long = 0, partBrand: String = "")
 
  /**
   * 获取询价数据
   */
  private def getInquiryPartInfo = {
    var partitionColumn = "DAMAGE_ID"
    options += ("url" -> jiaanpeiDb); options += ("user" -> jiaanpeiDbUser); options += ("password" -> jiaanpeiDbPwd)
    options += ("dbtable" -> "jc_shop_damage")
    options += ("partitionColumn" -> partitionColumn); options += ("lowerBound" -> "1"); options += ("upperBound" -> config.getUpperBound(options, partitionColumn).toString); options += ("numPartitions" -> "50") 
    
    /**
     * 查询定损单数据
     */
    //查询数据
    val startTime = date + " 00:00:00"; val endTime = date + " 23:59:59"
    spark.read.format("jdbc").options(options)
      .load.na.drop(Array("creat_time","CARNO")).createTempView("jc_shop_damage")
    val jcShopDamageDf = spark.sql(""
        + "SELECT "
          + "DAMAGE_ID as damage_id,"
          + "CARTYPE as car_type "
        + "FROM jc_shop_damage "
        + "WHERE CARNO NOT LIKE '测%' "
        + "AND creat_time BETWEEN timestamp('" + startTime + "') AND timestamp('" + endTime + "')")
    
    /**
     * 查询定损单配件
     */
    partitionColumn = "DAMAGE_PARTS_ID"
    options += ("dbtable" -> "jc_shop_damage_parts")
    options += ("partitionColumn" -> partitionColumn); options += ("lowerBound" -> "1"); options += ("upperBound" -> config.getUpperBound(options, partitionColumn).toString); options += ("numPartitions" -> "50") 
    spark.read.format("jdbc").options(options)
      .load.na.drop(Array("PARTOE")).createTempView("jc_shop_damage_parts")
      
    val jcShopDamagePartDf = spark.sql(""
      + "SELECT "
        + "DAMAGE_ID as damage_id,"
        + "DAMAGE_PARTS_ID as part_id,"
        + "PARTNAME as part_name,"
        + "PARTOE as part_oe "
      + "FROM jc_shop_damage_parts "
      + "WHERE PARTOE NOT LIKE 'JY%' AND PARTOE != ''")
    
      
    val removeSpecialCharacterFunc = udf(config.removeSpecialCharacter, StringType)
    val inquiryPartDf = jcShopDamageDf.as("df1")
      .join(jcShopDamagePartDf.as("df2"),Seq("damage_id"),"inner")
      .select(col("df1.damage_id"),col("df2.part_id"),col("df2.part_name"),col("df2.part_oe"),col("df1.car_type"))
      .withColumn("temp_oe", removeSpecialCharacterFunc(col("df2.part_oe")))
    
    /**
     * 匹配产品库中标准名称
     */
    partitionColumn = "id"
    options += ("url" -> productDb); options += ("user" -> productDbUser); options += ("password" -> productDbPwd)
    options += ("dbtable" -> "janpb_parts_product_info")
    options += ("partitionColumn" -> partitionColumn); options += ("lowerBound" -> "1"); options += ("upperBound" -> config.getUpperBound(options, partitionColumn).toString); options += ("numPartitions" -> "50")     
    
    val productInfoDf = spark.read.format("jdbc").options(options)
      .load.na.drop(Array("partsName"))
    
    val inquiryPartWithStandardNameDf = inquiryPartDf.as("df1")
      .join(productInfoDf.as("df2"), inquiryPartDf("temp_oe") === productInfoDf("partsOe"), "left")
      .select(col("df1.part_id"),
          col("df2.partsName") as "standard_part_name",
          col("df1.part_name"),
          col("df1.part_oe"),
          col("df1.car_type")
          )
      .na.drop(Array("standard_part_name")).distinct.toDF
          
    /**
     * 插入询价配件信息
     */
    options += ("dbtable" -> "pa_base_part")
    options -= ("partitionColumn"); options -= ("lowerBound"); options -= ("upperBound"); options -= ("numPartitions")
    inquiryPartWithStandardNameDf
      .foreachPartition(iter => {
        val list = new ListBuffer[InquiryPartInfo]
        iter.foreach(row => {
          val partId = row.getAs[Long]("part_id")
          val standardPartName = row.getAs[String]("standard_part_name")
          val partName = row.getAs[String]("part_name")
          val partOe = row.getAs[String]("part_oe")
          val vehicleType = row.getAs[String]("car_type")
          list.append(InquiryPartInfo(partId,standardPartName,partName,partOe,vehicleType))
        })
        var conn: Connection = null
        var stmt: Statement = null
        var sql = ""
        try{
          conn = DriverManager.getConnection(partDb,partDbUser,partDbPwd)
          stmt = conn.createStatement
          var sql = ""
          for(ele <- list){
            sql = (""
                + "INSERT INTO " + options("dbtable") + " (part_id,standard_part_name,part_name,oe,vehicle_type,data_source,create_time,update_time) "
                + "VALUE (" + ele.partId + ",'" + ele.standardPartName + "','" + ele.partName + "','" + ele.partOe + "','" + ele.vehicleType + "','1',NOW(),NOW()) "
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
     
    /**
     * 获取当天配件报价表
     */
    
    partitionColumn = "damagePartId"
    options += ("url" -> jiaanpeiDb); options += ("user" -> jiaanpeiDbUser); options += ("password" -> jiaanpeiDbPwd)
    options += ("dbtable" -> "jc_shop_damage_parts_prices")
    options += ("partitionColumn" -> partitionColumn); options += ("lowerBound" -> "1"); options += ("upperBound" -> config.getUpperBound(options, partitionColumn).toString); options += ("numPartitions" -> "50") 
        
    spark.read.format("jdbc").options(options)
      .load.na.drop(Array("remark"))
      .createTempView("jc_shop_damage_parts_prices")
   
    spark.sql(""
        + "SELECT "
          + "damagePartId,"
          + "remark "
        + "FROM jc_shop_damage_parts_prices "
        + "WHERE updateTime BETWEEN timestamp('" + startTime + "') AND timestamp('" + endTime + "') "
        + "AND remark != '' "
        + "AND status = '1'").createTempView("offer_part_temp")
      
    val offerPriceDf = spark.sql(""
        + "SELECT "
          + "damagePartId as damage_part_id," 
          + "CONCAT_WS(',', collect_set(left(remark,char_length(remark) - 1))) as part_brand "
        + "FROM offer_part_temp "
        + "GROUP BY damagePartId")
    val offerPartBrandDf = offerPriceDf.selectExpr("cast(damage_part_id as String) damage_part_id","part_brand")

    partitionColumn = "id"
    options += ("url" -> partDb); options += ("user" -> partDbUser); options += ("password" -> partDbPwd)
    options += ("dbtable" -> "pa_base_part")
    options += ("partitionColumn" -> partitionColumn); options += ("lowerBound" -> "1"); options += ("upperBound" -> config.getUpperBound(options, partitionColumn).toString); options += ("numPartitions" -> "50") 
        
    spark.read.format("jdbc").options(options).load.createTempView("base_part")
    val inquiryPartDfTemp = spark.sql(""
        + "SELECT id,part_id FROM base_part WHERE data_source = '1'")
    val partBrandDf = inquiryPartDfTemp.as("df1")
      .join(offerPartBrandDf.as("df2"), inquiryPartDfTemp("part_id") === offerPartBrandDf("damage_part_id"), "left")
      .select(col("df1.id"), col("df2.part_brand")).na.drop(Array("df2.part_brand"))
    
    options -= ("partitionColumn"); options -= ("lowerBound"); options -= ("upperBound"); options -= ("numPartitions")
    partBrandDf.foreachPartition(iter => {
      val list = new ListBuffer[OfferPartBrand]
      iter.foreach(row => {
        val id = row.getAs[Long]("id")
        val partBrand = row.getAs[String]("part_brand")
        list.append(OfferPartBrand(id,partBrand))
      })
      var conn: Connection = null
      var stmt: Statement = null
      var sql = ""
      try{
          conn = DriverManager.getConnection(partDb,partDbUser,partDbPwd)
          stmt = conn.createStatement
          var sql = ""
          for(ele <- list){
            sql = (""
                + "UPDATE " + options("dbtable") + " SET "
                  + "part_brand = '" + ele.partBrand + "',"
                  + "update_time = NOW() "
                + "WHERE id = " + ele.id + ";")
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
  
  def runApp{
    getInquiryPartInfo
  }
}