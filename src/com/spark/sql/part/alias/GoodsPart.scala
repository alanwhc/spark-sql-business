package com.spark.sql.part.alias

/**
 * 商品库配件
 */
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import scala.collection.mutable.ListBuffer
import java.sql.{Connection,Statement,DriverManager,SQLException}
import java.text.SimpleDateFormat

class GoodsPart(
  val sparkSession: SparkSession = null,
  val optionsMap: Map[String,String] = null,
  val dates: List[String] = null,
  val configs: Map[String,List[String]] = null
  ) extends Serializable {
  
  private val spark = sparkSession
  private val options = optionsMap
  private val date = dates(0)  
  private val year = dates(1)
  private val month = dates(2)
  private val day = dates(3)
  private val partDb = configs("url")(1)
  private val partDbUser = configs("user")(1)
  private val partDbPwd = configs("password")(1)
  private val productDb = configs("url")(2)
  private val productDbUser = configs("user")(2)
  private val productDbPwd = configs("password")(2)
  private var config = new Config()
  
  /**
   * 商品库数据类
   */
  case class GoodsPartInfo(partId: String = "", standardPartName: String = "", partName: String = "", partOe: String = "", vehicleType: String = "", partBrand: String = "")
  
  private def goodsPart = {
    var partitionColumn = "id"
    options += ("url" -> productDb); options += ("user" -> productDbUser); options += ("password" -> productDbPwd)
    options += ("dbtable" -> "janpb_goods_supp_standard_info")
    options += ("partitionColumn" -> partitionColumn); options += ("lowerBound" -> "1"); options += ("upperBound" -> config.getUpperBound(options, partitionColumn).toString); options += ("numPartitions" -> "50") 
    
    val startTime = date + " 00:00:00"; val endTime = date + " 23:59:59"  //设置起始和结束时间
    
    /**
     * 读取昨日上传商品数据
     */
    spark.read.format("jdbc").options(options).load.na.drop(Array("partsName")).createTempView("goods_initial")
    val goodsInitialDf = spark.sql(""
        + "SELECT "
          + "goodsCode,"
          + "partsName,"
          + "suppPartsName,"
          + "suppPartsOe,"
          + "vehicleType,"
          + "partsBrandName "
        + "FROM goods_initial "
        + "WHERE updateTime BETWEEN timestamp('" + startTime + "') AND timestamp('" + endTime + "') "
        + "AND status = '1'")
        
    options += ("dbtable" -> "pa_base_part")
    goodsInitialDf.foreachPartition(iter => {
      val list = new ListBuffer[GoodsPartInfo]
      iter.foreach(row => {
        val partId: String = row.getAs("goodsCode")
        val standardPartName: String = row.getAs("partsName")
        val partName: String = row.getAs("suppPartsName")
        val partOe: String = row.getAs("suppPartsOe")
        val vehicleType: String = row.getAs("vehicleType")
        val partBrand: String = row.getAs("partsBrandName")
        list.append(GoodsPartInfo(partId,standardPartName,partName,partOe,vehicleType,partBrand))
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
                + "INSERT INTO " + options("dbtable") + "(part_id,standard_part_name,part_name,oe,vehicle_type,part_brand,data_source,create_time,update_time) "
                + "VALUE ('" + ele.partId + "','" + ele.standardPartName + "','" + ele.partName + "','" + ele.partOe + "','" + ele.vehicleType + "','" + ele.partBrand + "','2',NOW(),NOW()) "
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
    spark.catalog.dropTempView("goods_initial")
  }
  
  def runApp{
    goodsPart
  }
}