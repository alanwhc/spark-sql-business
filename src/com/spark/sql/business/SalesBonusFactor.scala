package com.spark.sql.business

/**
 * 区域人员业绩提成系数计算
 */

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import java.sql.{Connection,Statement,SQLException,DriverManager,Date,Timestamp}
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._
import java.util.Calendar
import org.apache.spark.sql.types.{DoubleType,IntegerType,StringType}
import java.sql.PreparedStatement
import org.apache.avro.LogicalTypes.Decimal
import org.spark_project.dmg.pmml.False

class SalesBonusFactor(
    val sparkSession: SparkSession,
    val optionsMap: Map[String,String],
    val dates: List[String],
    val urls: List[String]
    )extends Serializable{
  
  @transient val spark = sparkSession
  @transient val options = optionsMap
  @transient val jiaanpeiReportDb = urls(0)
  @transient val salesDb = urls(1)
  @transient val usersDb = urls(2)
  @transient val factorObj = new BonusFactorFunction
  @transient val config = new Configuration
  
  val salesClassFunc = udf(factorObj.getSalesClass, StringType)
  val bonusFactorFunc = udf(factorObj.getBonusFactor, DoubleType)
  val salesClassNameFunc = udf(factorObj.getSalesClassName, StringType)
  val managerSelfopFunc = udf(factorObj.getManagerSelfopBonusFactor, DoubleType)
  val managerMarriedFunc = udf(factorObj.getManagerMarriedBonusFactor, DoubleType)
  val repeatShopFunc = udf(factorObj.getRepeatShopFactor, DoubleType)
  val selfopCompletionSelfopFunc = udf(factorObj.getCompletionSelfopRatio, DoubleType)
  val selfopCompletionMarriedFunc = udf(factorObj.getCompletionMarriedRatio, DoubleType)
  val date = dates(0);val year = dates(1); val month = dates(2); val day = dates(3)

  case class BaseDataFrame(){
    /**
    * 计算人保确认收货、确认实收金额
    */
    def businessData = {
      options += ("url" -> salesDb)
      options += ("dbtable" -> "manager_achievement_statistics")
      options += ("partitionColumn" -> "id")
      options += ("lowerBound" -> "1")
      options += ("upperBound" -> config.getUpperBound(options).toString)
      options += ("numPartitions" -> "50")
      
      val startDate = config.getDateValue(options, date, "firstdayinmonth")
      val managerDataDf = spark.read.format("jdbc").options(options).load
        .filter(row => {
          val managerId = row.getAs[Long]("manager_id")
          val dt = row.getAs[Date]("dt")
          if(managerId == 0)
            false
          else if(dt.getTime < startDate.getTime || dt.getTime > new SimpleDateFormat("yyyy-MM-dd").parse(date).getTime)
            false
          else
            true
        })
        
      //省份维度人保交易额、人保实收交易额
      val provincePiccDataDf =  managerDataDf.groupBy("province")
        .agg(
          sum(col("picc_selfop_order") + col("picc_married_order")) as "amount_picc",
          sum(col("picc_selfop_actual_order") + col("picc_married_actual_order")) as "amount_actual_picc"
          )
      
      //客户经理维度人保交易额、人保实收交易额
      val managerPiccDataDf = managerDataDf.groupBy("manager_id")
        .agg(
          sum(col("picc_selfop_order") + col("picc_married_order")) as "amount_picc",
          sum(col("picc_selfop_actual_order") + col("picc_married_actual_order")) as "amount_actual_picc"
          )
          
      //省份维度人保自营交易额、人保自营实收交易额
      val provincePiccSelfopDf = managerDataDf.groupBy("province")
        .agg(
          sum(col("picc_selfop_order")) as "amount_picc_selfop",
          sum(col("picc_selfop_actual_order")) as "amount_actual_picc_selfop"
          )
      (provincePiccDataDf,managerPiccDataDf,provincePiccSelfopDf)
    }
    
    /**
     * 大区负责人、省级负责人、运营岗、供应链人员基本信息DataFrame
     */
    def salesPersonel = {
      options += ("url" -> salesDb)
      options += ("dbtable" -> "data_sales_bonus_coefficient1")
      options += ("partitionColumn" -> "id")
      options += ("lowerBound" -> "1")
      options += ("upperBound" -> config.getUpperBound(options).toString)
      options += ("numPartitions" -> "50")
      
      val personDf = spark.read.format("jdbc").options(options).load
      //大区负责人、省级负责人、运营岗、供应链
      val regularPersonDf = personDf
        .filter(row => {
          val position = row.getAs[String]("position"); val yr = row.getAs[String]("year"); val mth = row.getAs[String]("month")
          if(!List("00","01","02","05").contains(position))
            false
          else if(!yr.equals(year) || !mth.equals(month.drop(1)))
            false
          else
            true
        })
      
      //客户经理
      val managerDf = personDf
        .filter(row => {
          val position = row.getAs[String]("position"); val yr = row.getAs[String]("year"); val mth = row.getAs[String]("month")
          if(!List("03").contains(position))
            false
          else if(!yr.equals(year) || !mth.equals(month.drop(1)))
            false
          else
            true
        })
      (regularPersonDf,managerDf)
    }
    
    /**
     * 获取省份维度人保、人保自营目标
     */
    def provinceTarget = {
      //获取交易目标数据
      options += ("url" -> jiaanpeiReportDb)
      options += ("dbtable" -> "sales_target_discomposition1")
      options += ("partitionColumn" -> "id")
      options += ("lowerBound" -> "1")
      options += ("upperBound" -> config.getUpperBound(options).toString)
      options += ("numPartitions" -> "50")
      val businessTargetDf = spark.read.format("jdbc").options(options).load
        .filter(row => {
          val yr: Integer = row.getAs("year"); val mth: Integer = row.getAs("month"); val city: String = row.getAs("city")
          if(yr.toString.equals(year) && mth.toString.equals(month.drop(1)) && city.equals("100000"))
            true
          else
            false
        })
        
      val piccTarget = businessTargetDf.
      select(
          col("province"), 
          when(col("picc_selfop_trans_target").isNull,0).otherwise(col("picc_selfop_trans_target")) as "picc_selfop_target",
          when(col("picc_marriedDeal_trans_target").isNull,0).otherwise(col("picc_marriedDeal_trans_target")) as "picc_married_target")
      val piccSelfopTarget = businessTargetDf.select(col("province"), col("picc_selfop_trans_target")) 
      (piccTarget,piccSelfopTarget)
    }

    /**
     * 复购修理厂数据
     */
    def provinceRepeatShop = {
      options += ("url" -> salesDb)
      options += ("dbtable" -> "data_active_shop")
      options -= ("partitionColumn")
      options -= ("lowerBound")
      options -= ("upperBound")
      options -= ("numPartitions")
      val repeatShopDf = spark.read.format("jdbc").options(options).load
        .filter(row => {
          if(row.getAs[Date]("date").toString.equals(date))
            true
          else
            false
        })
      repeatShopDf
    }
  }
  
  /**
     * 插入MySQL数据库
     */
  @transient
  private def insertIntoTable(url: String, user: String, password: String, sql: String){
    var conn: Connection = null
    var stmt: Statement = null
    try{
      conn = DriverManager.getConnection(url, user, password)
      stmt = conn.createStatement
      stmt.execute(sql)
    }catch{
      case e: SQLException => e.printStackTrace
    }finally{
      if(stmt != null)
        stmt.close
      if(conn != null)
        conn.close
    }
  }
  
  /**
   * 区域人员信息
   */
  private def personelInfo {
    options += ("url" -> usersDb)
    options += ("dbtable" -> "base_sales_user")
    var url: String = ""; var user: String = ""; var password: String = ""; var tableName: String = ""
    
    val filterMap = Map(
        "userStatus" -> List("1"),
        "isRegionalLeader" -> List("0"),
        "isHeadQuarter" -> List("0"))
    
    /**
     * 省级负责人、运营岗、客户经理、供应链人员信息
     */
    val baseUserDf = spark.read.format("jdbc").options(options).load
      .na.drop(Array("user_position"))
      .filter(row => {
        if(filterMap("userStatus").size > 0 && !filterMap("userStatus").contains(row.getAs[String]("status")))
          false
        else if(filterMap("isRegionalLeader").size > 0 && !filterMap("isRegionalLeader").contains(row.getAs[String]("is_region_leader")))
          false
        else if(filterMap("isHeadQuarter").size > 0 && !filterMap("isHeadQuarter").contains(row.getAs[String]("is_hq")))
          false
        else
          true
      })
      
      options += ("dbtable" -> "sales_user_area")
      val baseAreaDf = spark.read.format("jdbc").options(options).load
      val baseUserAreaDf = baseUserDf.as("df1").join(baseAreaDf.as("df2"), Seq("user_id"),"left")
      .select(
          col("df1.user_id"),
          col("df1.fr_username"),
          col("df1.name"),
          col("df2.position"),
          col("df2.province"))
      .distinct
      
      //大区负责人相关信息
      options -= ("partitionColumn")
      options -= ("lowerBound")
      options -= ("upperBound")
      options -= ("numPartitions")
      options += ("dbtable" -> "base_sales_user")
      val userDf = spark.read.format("jdbc").options(options).load
      options += ("dbtable" -> "region_user")
      val regionUserDf = spark.read.format("jdbc").options(options).load
      options += ("dbtable" -> "region_province")
      val regionProvinceDf = spark.read.format("jdbc").options(options).load
      
      val tempDf1 = userDf.as("df1")
        .join(broadcast(regionUserDf).as("df2"), Seq("user_id"), "inner")
        .select(col("df1.user_id"),col("df1.fr_username"),col("df1.name"),col("df2.region_id"))
      val baseRegionUserDf = tempDf1.as("df1")
        .join(broadcast(regionProvinceDf).as("df2"), Seq("region_id"), "inner")
        .select(
            col("df1.user_id"),
            col("df1.fr_username"),
            col("df1.name"),
            col("df2.province"))
      
      //将人员信息插入数据库
      options += ("url" -> salesDb)
      options += ("dbtable" -> "data_sales_bonus_coefficient1")
      url = options("url"); user = options("user"); password = options("password"); tableName = options("dbtable")
      val repeatFactor = factorObj.getRepeatShopFactor(0.0)  //初始化修理厂复购率
      baseUserAreaDf.foreach(row => {
        val userId = row.getAs[Long]("user_id")
        val frUsername = row.getAs[String]("fr_username")
        val name = row.getAs[String]("name")
        val position = row.getAs[String]("position")
        val province = row.getAs[String]("province")
        
        val sql = (""
            + "INSERT INTO " + tableName + "(user_id,fr_username,name,position,province,repeat_shop_factor,year,month,create_time,update_time) "
            + "VALUES (" + userId + ",'" + frUsername + "','" + name + "','" + position + "','"
            + province + "'," + repeatFactor + ",'" + year + "','" + month.drop(1) +"',NOW(),NOW()) "
            + "ON DUPLICATE KEY UPDATE "
              + "user_id = " + userId)
        insertIntoTable(url, user, password, sql)
      })
      
      baseRegionUserDf.foreach(row => {
        val userId = row.getAs[Long]("user_id")
        val frUsername = row.getAs[String]("fr_username")
        val name = row.getAs[String]("name")
        val province = row.getAs[String]("province")
        
        val sql = (""
            + "INSERT INTO " + tableName + "(user_id,fr_username,name,position,province,repeat_shop_factor,year,month,create_time,update_time) "
            + "VALUES (" + userId + ",'" + frUsername + "','" + name + "','00','"
            + province + "'," + repeatFactor + ",'" + year + "','" + month.drop(1) +"',NOW(),NOW()) "
            + "ON DUPLICATE KEY UPDATE "
              + "user_id = " + userId)
        insertIntoTable(url, user, password, sql)
      })
  }

  /**
   * 计算大区负责人、省级负责人、运营岗、供应链提成比例
   */
  private def regularProfitFactor{
    val businessDataDf = new BaseDataFrame().businessData._1  //获取省份维度交易数据
    val regularPersonelDf = new BaseDataFrame().salesPersonel._1  //人员信息
    
    val personelBusinessDataDf = regularPersonelDf.as("df1").join(businessDataDf.as("df2"),Seq("province"),"left")
      .groupBy(col("df1.user_id"),col("df1.name"),col("df1.position"))
      .agg(
          sum(when(col("df2.amount_picc").isNull, 0).otherwise(col("df2.amount_picc"))) as "amount_picc",
          sum(when(col("df2.amount_actual_picc").isNull, 0).otherwise(col("df2.amount_actual_picc"))) as "amount_actual_picc")
    
    val personelBonusFactorDf = personelBusinessDataDf
      .withColumn("position_amount_actual_picc", concat(col("position"),col("amount_actual_picc")))  //岗位 + 人保实收金额
      .withColumn("sales_class", salesClassFunc(col("position_amount_actual_picc")))  //计算人员职级
      .drop(col("position_amount_actual_picc"))
      .withColumn("pos_class", concat(col("position"),col("sales_class")))  //岗位 + 职级
      .withColumn("bonus_ratio", bonusFactorFunc(col("pos_class")))  //计算毛利提成系数      
      .withColumn("sales_class_name", salesClassNameFunc(col("pos_class")))  //职级名称
      .drop(col("pos_class"))
    
    //更新提成系数
    options += ("url" -> salesDb)
    options += ("dbtable" -> "data_sales_bonus_coefficient1")
    val url = options("url"); val user = options("user"); val password = options("password"); val tableName = options("dbtable")
    personelBonusFactorDf.foreach(row => {
      val userId: Long = row.getAs("user_id")
      val name: String = row.getAs("name")
      val position: String = row.getAs("position")
      val amountPicc: java.math.BigDecimal = row.getAs("amount_picc")
      val amountActualPicc: java.math.BigDecimal = row.getAs("amount_actual_picc")
      val salesClass: String = row.getAs("sales_class")
      val bonusRatio: Double = row.getAs("bonus_ratio")
      val className: String = row.getAs("sales_class_name")
      
      val sql = (""
          + "UPDATE " + tableName + " SET "
            + "personal_picc_amount = " + amountActualPicc + ","
            + "personal_picc_actual_amount = " + amountActualPicc + ","
            + "married_profit_bonus_ratio = " + bonusRatio + ","
            + "selfop_profit_bonus_ratio = " + bonusRatio + ","
            + "manager_level = '" + salesClass + "',"
            + "level_name = '" + className + "',"
            + "update_time = NOW() "
          + "WHERE year = '" + year + "' AND month = '" + month.drop(1) + "' AND position IN ('00','01','02','05') AND user_id = " + userId + ";")
      insertIntoTable(url, user, password, sql)
    })
  }
  
  /**
   * 计算大区负责人兼任省级负责人的毛利提成比例
   */
  private def parttimeProvincialProfitFactor{
    options += ("url" -> usersDb)
    options += ("dbtable" -> "region_user")
    options -= ("partitionColumn")
    options -= ("lowerBound")
    options -= ("upperBound")
    options -= ("numPartitions")
    val regionUserDf = spark.read.format("jdbc").options(options).load
      .filter(row => {
        if(row.getAs[String]("status") == "0")
          false
        else
          true
      })
    
    options += ("dbtable" -> "sales_user_area")
    options += ("partitionColumn" -> "id")
    options += ("lowerBound" -> "1")
    options += ("upperBound" -> config.getUpperBound(options).toString)
    options += ("numPartitions" -> "50")
    val userAreaDf = spark.read.format("jdbc").options(options).load
      .filter(row => {
        if(row.getAs[String]("status") == "0")
          false
        else
          true
      })
    
    val businessDataDf = new BaseDataFrame().businessData._1
    
    val regionUserAreaDf = regionUserDf.as("df1").join(userAreaDf.as("df2"), Seq("user_id"),"inner")
      .select(col("df1.user_id"),col("df1.name"), col("df2.province"),col("df2.position"))
    val regionUserProvinceDataDf = regionUserAreaDf.as("df1").join(businessDataDf.as("df2"), Seq("province"),"inner")
      .select(
          col("df1.user_id"),
          col("df1.name"),
          col("df1.province"),
          col("df1.position"),
          col("df2.amount_picc"),
          col("df2.amount_actual_picc"),
          concat(col("df1.position"),col("df2.amount_actual_picc")) as "pos_amount_actual_picc")
      .withColumn("sales_class", salesClassFunc(col("pos_amount_actual_picc")))
      .drop(col("pos_amount_actual_picc"))
      .withColumn("pos_class", concat(col("position"),col("sales_class")))
      .withColumn("bonus_ratio", bonusFactorFunc(col("pos_class")))
      .drop(col("sales_class"))
      .drop(col("pos_class"))
      .drop(col("position"))
      
      options += ("url" -> salesDb)
      options += ("dbtable" -> "data_sales_bonus_coefficient1")
      val year = dates(1); val month = dates(2)
      val url = options("url"); val user = options("user"); val password = options("password"); val tableName = options("dbtable")
      regionUserProvinceDataDf.foreach(row => {
        val userId: Integer = row.getAs("user_id")
        val bonusRatio: Double = row.getAs("bonus_ratio")
        val province: String = row.getAs("province")
        
        val sql = (""
          + "UPDATE " + tableName + " SET "
            + "married_profit_bonus_ratio = " + bonusRatio + ","
            + "selfop_profit_bonus_ratio = " + bonusRatio + ","
            + "update_time = NOW() "
          + "WHERE year = '" + year + "' AND month = '" + month.drop(1) + "' "
          + "AND position = '00' AND user_id = " + userId + " AND province = '" + province + "'")
          
        insertIntoTable(url, user, password, sql)
      })
  }
  
  /**
   * 计算客户经理职级及毛利提成系数
   */
  private def customerManagerProfitFator{
    val businessDataDf = new BaseDataFrame().businessData._2  //按客户经理维度计算交易额
    val managerDf = new BaseDataFrame().salesPersonel._2  //客户经理信息
    
    val managerBusinessDataDf = managerDf.as("df1").join(businessDataDf.as("df2"),managerDf("user_id") === businessDataDf("manager_id"),"left")
      .select(
          col("df1.user_id"),col("df1.name"),col("df1.position"),
          when(col("df2.amount_picc").isNull, 0).otherwise(col("df2.amount_picc")) as "amount_picc",
          when(col("df2.amount_actual_picc").isNull, 0).otherwise(col("df2.amount_actual_picc")) as "amount_actual_picc")
      .withColumn("pos_amount_actual_picc", concat(col("position"),col("amount_actual_picc")))
      .withColumn("sales_class", salesClassFunc(col("pos_amount_actual_picc")))
      .drop("pos_amount_actual_picc")
      .withColumn("pos_class", concat(col("position"),col("sales_class")))
      .withColumn("selfop_bonus_ratio", managerSelfopFunc(col("sales_class")))
      .withColumn("married_bonus_ratio", managerMarriedFunc(col("sales_class")))
      .withColumn("sales_class_name", salesClassNameFunc(col("pos_class")))
      .drop(col("pos_class"))
    
    options += ("url" -> salesDb)
    options += ("dbtable" -> "data_sales_bonus_coefficient1")
    val url = options("url"); val user = options("user"); val password = options("password"); val tableName = options("dbtable")
    
    managerBusinessDataDf.foreach(row =>{
      val userId: Long = row.getAs("user_id")
      val name: String = row.getAs("name")
      val amountPicc: java.math.BigDecimal = row.getAs("amount_picc")
      val amountActualPicc: java.math.BigDecimal = row.getAs("amount_actual_picc")
      val salesClass: String = row.getAs("sales_class")
      val selfopBonus: Double = row.getAs("selfop_bonus_ratio")
      val marriedBonus: Double = row.getAs("married_bonus_ratio")
      val className: String = row.getAs("sales_class_name")
      
      val sql = (""
          + "UPDATE " + tableName + " SET "
            + "personal_picc_amount = " + amountPicc + ","
            + "personal_picc_actual_amount = " + amountActualPicc + ","
            + "married_profit_bonus_ratio = " + marriedBonus + ","
            + "selfop_profit_bonus_ratio = " + selfopBonus + ","
            + "manager_level = '" + salesClass + "',"
            + "level_name = '" + className + "',"
            + "update_time = NOW() "
          + "WHERE year = '" + year + "' AND month = '" + month.drop(1) + "' "
          + "AND position = '03' AND user_id = " + userId + "")
          
    insertIntoTable(url, user, password, sql)
    })
  }
  
  /**
   * 省级负责人、运营岗、供应链人保自营完成比例及提成系数，修理厂复购率
   */
  private def regularPiccSelfopRepeatShop{    
    //获取业务数据
    val businessDataDf = new BaseDataFrame().businessData._3
    
    //省份人保自营目标
    val businessTargetDf = new BaseDataFrame().provinceTarget._2
    
    //复购修理厂数据
    val repeatShopDf = new BaseDataFrame().provinceRepeatShop
    
    val businessDataTargetDf = businessDataDf.as("df1").join(broadcast(businessTargetDf).as("df2"),Seq("province"),"left")
      .select(
          col("df1.province"),
          col("df1.amount_picc_selfop"),
          col("df1.amount_actual_picc_selfop"),
          when(col("df2.picc_selfop_trans_target").equalTo(0), 100).otherwise(
              round(col("df1.amount_actual_picc_selfop") / when(col("df2.picc_selfop_trans_target").isNull, 1).otherwise(col("df2.picc_selfop_trans_target")) * 100
                  ,2)
            ) as "piccSelfopCompletion")
    
    val piccSelfopCompletionRepeatShopDf = businessDataTargetDf.as("df1").join(repeatShopDf.as("df2"),Seq("province"),"left")
      .select(
          col("df1.province"),
          col("df1.piccSelfopCompletion") as "complete_ratio",
          when(col("df2.previous_repeat_shop").isNull || col("df2.previous_repeat_shop").equalTo(0),0)
            .otherwise(round(col("df2.this_month_repeat_shop") / col("df2.previous_repeat_shop") * 100, 2)) as "repeat_ratio")
      .withColumn("selfop_factor", selfopCompletionSelfopFunc(col("complete_ratio")))
      .withColumn("married_factor", selfopCompletionMarriedFunc(col("complete_ratio")))
      .withColumn("repeat_shop_factor", repeatShopFunc(col("repeat_ratio")))
    
    options += ("url" -> salesDb)
    options += ("dbtable" -> "data_sales_bonus_coefficient1")
    val url = options("url"); val user = options("user"); val password = options("password"); val tableName = options("dbtable")
    piccSelfopCompletionRepeatShopDf.foreach(row => {
      val province: String = row.getAs("province")
      val completeRatio: Double = row.getAs("complete_ratio")
      val repeatRatio: Double = row.getAs("repeat_ratio")
      val selfopFactor: Double = row.getAs("selfop_factor"); val marriedFactor: Double = row.getAs("married_factor")
      val repeatFactor: Double = row.getAs("repeat_shop_factor")
      
      var sql = (""
          + "UPDATE " + tableName + " SET "
            + "picc_selfop_completion = " + completeRatio + ","
            + "this_repeat_rate = " + repeatRatio + ","
            + "selfop_profit_factor = " + selfopFactor + ","
            + "married_profit_factor = " + marriedFactor + ","
            + "repeat_shop_factor = " + repeatFactor + ","
            + "update_time = NOW() "
          + "WHERE year = '" + year + "' AND month = '" + month.drop(1) + "' "
          + "AND position IN ('01','02','05') AND province = '" + province + "'")
      insertIntoTable(url, user, password, sql)
      
      sql = (""
          + "UPDATE " + tableName + " SET "
            + "picc_selfop_completion = " + completeRatio + ","
            + "selfop_profit_factor = " + selfopFactor + ","
            + "married_profit_factor = " + marriedFactor + ","
            + "update_time = NOW() "
          + "WHERE year = '" + year + "' AND month = '" + month.drop(1) + "' "
          + "AND position = '03' AND province = '" + province + "'")
      insertIntoTable(url, user, password, sql)
    })
  }
  
  /**
   * 大区负责人自营完成比例、修理厂复购率提成系数
   */
  private def regionPiccSelfopRepeatShop {
    //省份维度人保自营数据
    val provincePiccSelfopDataDf = new BaseDataFrame().businessData._3
    
    //各省份人保自营目标
    val provincePiccSelfopTargetDf = new BaseDataFrame().provinceTarget._2
    
    //各省份人保目标
    val provincePiccTargetDf = new BaseDataFrame().provinceTarget._1
    
    //各省修理厂复购数据
    val repeatShopDf = new BaseDataFrame().provinceRepeatShop
    
    options += ("url" -> usersDb)
    options += ("dbtable" -> "region_province")
    options -= ("partitionColumn")
    options -= ("lowerBound")
    options -= ("upperBound")
    options -= ("numPartitions")
    
    val regionProvinceDf1 = spark.read.format("jdbc").options(options).load
    val regionProvinceDf = regionProvinceDf1
      .filter(row => row.getAs[String]("is_responsible").equals("0"))
      
    options += ("dbtable" -> "region_user")
    val regionUserDf = spark.read.format("jdbc").options(options).load
      .filter(row => row.getAs[String]("status").equals("1"))
    
    options += ("dbtable" -> "base_sales_user")
    val baseUserDf = spark.read.format("jdbc").options(options).load
      .filter(row => row.getAs[String]("status").equals("1"))
    
    //计算大区级人保交易额目标
    val piccTargetProvinceDf = provincePiccTargetDf.as("df1").join(broadcast(regionProvinceDf).as("df2"),Seq("province"),"inner")
      .groupBy(col("df2.region_id"))
      .agg(sum(col("df1.picc_selfop_target") + col("df1.picc_married_target")) as "picc_target")
      
    val regionPiccTargetDf = piccTargetProvinceDf.as("df1").join(broadcast(regionUserDf).as("df2"),Seq("region_id"),"inner")
      .select(col("df1.region_id"), col("df2.user_id"),col("df2.region_name"),col("df2.name"),col("df1.picc_target"))
    
    //计算大区人保自营交易额，修理厂复购率
    val regionProvinceTargetDf = provincePiccTargetDf.as("df1").join(broadcast(regionProvinceDf).as("df2"),Seq("province"),"inner")
      .select(col("df2.region_id"), col("df1.province"),col("df1.picc_selfop_target"),
          col("df1.picc_selfop_target") + col("df1.picc_married_target") as "picc_target")
          
    val regionBusinessTargetDf = regionProvinceTargetDf.as("df1").join(provincePiccSelfopDataDf.as("df2"),Seq("province"),"left")
      .select(col("df1.region_id"),col("df1.province"),col("df1.picc_selfop_target"),col("df1.picc_target"),
          when(col("df2.amount_picc_selfop").isNull,0).otherwise(col("df2.amount_picc_selfop")) as "picc_selfop",
          when(col("df2.amount_actual_picc_selfop").isNull,0).otherwise(col("df2.amount_actual_picc_selfop")) as "picc_actual_selfop")
          
    val regionBusinessTargetRepeatShopDf = regionBusinessTargetDf.as("df1").join(repeatShopDf.as("df2"),Seq("province"),"left")
      .select(col("df1.region_id"),col("df1.province"),col("df1.picc_selfop_target"),col("df1.picc_target"),
          col("df1.picc_selfop"),col("df1.picc_actual_selfop"),col("df2.this_month_repeat_shop"),col("df2.previous_repeat_shop"))
    
    //聚合计算大区内各省份权重人保自营完成率，修理厂复购率
    val regionCompletionRepeatShopDf = regionBusinessTargetRepeatShopDf.as("df1").join(regionPiccTargetDf.as("df2"),Seq("region_id"),"inner")
      .select(
          col("df1.region_id"),col("df1.province"),col("df2.user_id"),col("df2.region_name"),col("df2.name"),
          col("df1.picc_actual_selfop") / col("df1.picc_selfop_target") as "picc_selfop_completion",
          when(col("df1.previous_repeat_shop").equalTo(0),0).otherwise(col("df1.this_month_repeat_shop") / col("df1.previous_repeat_shop")) as "repeat_shop_completion",
          col("df1.picc_target") / col("df2.picc_target") as "picc_target_weight")
      .groupBy(col("user_id"))
      .agg(
          round(sum(col("picc_selfop_completion") * col("picc_target_weight")) * 100,2) as "complete_ratio",
          round(sum(col("repeat_shop_completion") * col("picc_target_weight")) * 100,2) as "repeat_ratio")
          
    //大区基本信息
    val baseUserRegionDf = regionUserDf.as("df1")
      .join(baseUserDf.as("df2"),Seq("user_id"),"inner")
      .join(regionProvinceDf1.as("df3"),regionUserDf("region_id") === regionProvinceDf1("region_id"),"inner")
      .select(
          col("df1.region_id"),col("df3.province"),col("df1.user_id"),col("df1.name"),col("df3.is_responsible"))
    
    //计算大区人保自营完成率、修理厂复购率，及提成系数
    val regionBonusFactor = baseUserRegionDf.as("df1")
      .join(provincePiccSelfopTargetDf.as("df2"), baseUserRegionDf("province") === provincePiccSelfopTargetDf("province"),"left")
      .join(repeatShopDf.as("df3"),baseUserRegionDf("province") === repeatShopDf("province"),"left")
      .join(provincePiccSelfopDataDf.as("df4"),repeatShopDf("province") === provincePiccSelfopDataDf("province"),"left")
      .join(regionCompletionRepeatShopDf.as("df5"),baseUserRegionDf("user_id") === regionCompletionRepeatShopDf("user_id"),"left")
      .select(
          col("df1.region_id"),col("df1.province"),col("df1.user_id"),
          when(col("df1.is_responsible").equalTo("1"), 
              round(when(col("df2.picc_selfop_trans_target").equalTo(0), 100).otherwise(
                  when(col("df4.amount_actual_picc_selfop").isNull,0).otherwise(col("df4.amount_actual_picc_selfop")) / col("df2.picc_selfop_trans_target")) * 100,2))
                  .otherwise(when(col("df5.complete_ratio").isNull, 0).otherwise(col("df5.complete_ratio"))) as "complete_ratio",
          when(col("df1.is_responsible").equalTo("1"),
              round(col("df3.this_month_repeat_shop") / col("df3.previous_repeat_shop") * 100,2)).otherwise(when(col("df5.repeat_ratio").isNull, 0).otherwise(col("df5.repeat_ratio"))) as "repeat_ratio"
              )
      .withColumn("selfop_factor", selfopCompletionSelfopFunc(col("complete_ratio")))
      .withColumn("married_factor", selfopCompletionMarriedFunc(col("complete_ratio")))
      .withColumn("repeat_shop_factor", repeatShopFunc(col("repeat_ratio")))
    
    //更新数据
    options += ("url" -> salesDb)
    options += ("dbtable" -> "data_sales_bonus_coefficient1")
    val url = options("url"); val user = options("user"); val password = options("password"); val tableName = options("dbtable")
    regionBonusFactor.foreach(row => {
      val userId: Integer = row.getAs("user_id"); val province: String = row.getAs("province")
      val completeRatio: Double = row.getAs("complete_ratio")
      val repeatRatio: Double = row.getAs("repeat_ratio")
      val selfopFactor: Double = row.getAs("selfop_factor")
      val marriedFactor: Double = row.getAs("married_factor")
      val repeatShopFactor: Double = row.getAs("repeat_shop_factor")
      
      val sql = (""
          + "UPDATE " + tableName + " SET "
            + "picc_selfop_completion = " + completeRatio + ","
            + "this_repeat_rate = " + repeatRatio + ","
            + "selfop_profit_factor = " + selfopFactor + ","
            + "married_profit_factor = " + marriedFactor + ","
            + "repeat_shop_factor = " + repeatShopFactor + ","
            + "update_time = NOW() "
          + "WHERE year = '" + year + "' AND month = '" + month.drop(1) + "' "
          + "AND user_id = " + userId + " AND province = '" + province + "'")
      insertIntoTable(url, user, password, sql)
    })
  }
  
  /**
   * 客户经理修理厂复购率
   */
  private def managerRepeatShopRate{
    options += ("url" -> salesDb)
    options += ("dbtable" -> "data_repeat_shop_detail")
    
    val repeatShopDetailDf = spark.read.format("jdbc").options(options).load
      .filter(row => row.getAs[String]("year").equals(year) && row.getAs[String]("month").equals(month.drop(1)))
    
    options += ("dbtable" -> "shop_manager_mapping")
    val shopManagerDf = spark.read.format("jdbc").options(options).load
    
    val managerRepeatShopDf = repeatShopDetailDf.as("df1").join(shopManagerDf.as("df2"),Seq("shop_id"),"inner")
      .groupBy(col("df2.manager_id"))
      .agg(
          round(sum(when(col("df1.type").equalTo("1"), 1).otherwise(0)) / sum(when(col("df1.type").equalTo("2"), 1).otherwise(0))
              * 100,2) as "repeat_ratio")
      .withColumn("repeat_shop_factor", repeatShopFunc(col("repeat_ratio")))
    
    options += ("dbtable" -> "data_sales_bonus_coefficient1")
    val url = options("url"); val user = options("user"); val password = options("password"); val tableName = options("dbtable")
    managerRepeatShopDf.foreach(row => {
      val userId: Long = row.getAs("manager_id")
      val repeatRatio: Double = row.getAs("repeat_ratio")
      val repeatShopFactor: Double = row.getAs("repeat_shop_factor")
      
      val sql = (""
          + "UPDATE " + tableName + " SET "
            + "this_repeat_rate = " + repeatRatio + ","
            + "repeat_shop_factor = " + repeatShopFactor + ","
            + "update_time = NOW() "
          + "WHERE year = '" + year + "' AND month = '" + month.drop(1) + "' "
          + "AND position = '03' AND user_id = " + userId + "")
      insertIntoTable(url, user, password, sql)
    })
  }
  
  /**
   * 计算各项系数
   */
  def salesBonusFactor{
    personelInfo  //插入更新人员基础信息
    regularProfitFactor  //计算大区负责人、省级负责人、运营岗、供应链毛利提成系数
    parttimeProvincialProfitFactor  //计算大区负责人兼任省级负责人毛利提成系数
    customerManagerProfitFator  //计算客户经理毛利提成系数
    regularPiccSelfopRepeatShop  //计算省级负责人、运营岗、供应链、客户经理人保自营完成率、修理厂复购率系数
    regionPiccSelfopRepeatShop  //计算大区负责人
    managerRepeatShopRate //客户经理修理厂复购率
    
    options += ("url" -> salesDb)
    val url = options("url"); val user = options("user"); val password = options("password")
    var sql = "TRUNCATE usersdb.data_sales_bonus_coefficient1"
    insertIntoTable(url, user, password, sql)  
    sql = "INSERT INTO usersdb.data_sales_bonus_coefficient1 SELECT * FROM data_sales_bonus_coefficient1"
    insertIntoTable(url, user, password, sql)
  }
}