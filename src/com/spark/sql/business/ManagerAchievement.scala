package com.spark.sql.business

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import java.sql.{Connection,Statement,SQLException,DriverManager,Date,Timestamp}
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._
import java.util.Calendar

/**
 * 客户经理业绩统计
 */

class ManagerAchievement(
    val sparkSession: SparkSession,
    val optionsMap: Map[String,String],
    val dates: List[String],
    val urls: List[String]
    ) extends Serializable {
    @transient val spark = sparkSession
    @transient val options = optionsMap
    @transient val date = dates
    @transient val jiaanpeiReportDb = urls(0)
    @transient val salesDb = urls(1)
    
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
     * 客户经理业绩统计
     */
    def managerAchievement {
      val config = new Configuration
      options += ("url" -> jiaanpeiReportDb)
      
      val allDates = dates
      val date = allDates(0);val year = allDates(1); val month = allDates(2); val day = allDates(3)
      val filterMap = Map(
            "date" -> List(allDates(0)),
            "b2b" -> List("1"),
            "cityStatus" -> List("1"))
            
      //匹配统计维度的地市
      options += ("dbtable" -> "report_dictionary_business")
      options += ("partitionColumn" -> "id")
      options += ("lowerBound" -> "1")
      options += ("upperBound" -> config.getUpperBound(options).toString)
      options += ("numPartitions" -> "50")
      val cityDf = spark.read.format("jdbc").options(options).load
      val validCityDf = cityDf.filter(row => {
          if(filterMap("cityStatus").size > 0 && !filterMap("cityStatus").contains(row.getAs[String]("status")))
              false
            else
              true
        })
      
      /*
       * 获取B2b基础数据
       */
      options += ("dbtable" -> "base_jap_order")
      options += ("partitionColumn" -> "id")
      options += ("lowerBound" -> "1")
      options += ("upperBound" -> config.getUpperBound(options).toString)
      options += ("numPartitions" -> "50")
      val baseB2bDataDf = spark.read.format("jdbc").options(options).load
        .filter(row => {
          if(filterMap("b2b").size > 0 && !filterMap("b2b").contains(row.getAs[String]("is_b2b")))
              false
            else
              true
        })
      
      //筛选确认收货的数据
      val forwardDf = baseB2bDataDf
        .na
        .drop(Array("deliver_date"))
        .filter(row => {
          val deliverDate = row.getAs[Date]("deliver_date").toString
          if(filterMap("date").size > 0 && !filterMap("date").contains(deliverDate))
            false
          else
            true
        })
        
      //筛选退货完成数据
      val backwardDf = baseB2bDataDf
        .na
        .drop(Array("refund_date","payment_date"))
        .filter(row => {
          val refundDate = row.getAs[Date]("refund_date").toString
          val paymentDate = row.getAs[Date]("payment_date")
          if(paymentDate.getTime >= new SimpleDateFormat("yyyy-MM-dd").parse("2018-08-01").getTime
              && !List("700","701").contains(row.getAs[String]("order_status")))
            false
          else if(filterMap("date").size > 0 && !filterMap("date").contains(refundDate))
            false
          else
            true
        })
        
      //将确认收货源数据地市替换为统计维度的地市
      val deliveredBaseDataDf = forwardDf.as("df1").join(validCityDf.as("df2"),forwardDf("city") === validCityDf("city_code"),"inner")
        .select(
           col("df1.province"),
           col("df2.business_city_code") as "city",
           col("df1.manager_id"),
           col("df1.manager_name"),
           col("df1.is_picc"),
           col("df1.is_fbj"),
           col("df1.order_amount"))
      
      //按省、市、客户经理维度聚合计算
      val forwardProvinceCityManagerDataDf = deliveredBaseDataDf.groupBy("province", "city","manager_id","manager_name")
        .agg(
            sum(col("order_amount")) as "b2bOrder",
            sum(when(col("is_picc").equalTo("1") && col("is_fbj").equalTo("1"), col("order_amount")).otherwise(0)) as "piccSelfop",
            sum(when(col("is_picc").equalTo("1") && col("is_fbj").equalTo("0"), col("order_amount")).otherwise(0)) as "piccMarried",
            sum(when(col("is_picc").equalTo("0") && col("is_fbj").equalTo("1"), col("order_amount")).otherwise(0)) as "nonPiccSelfop",
            sum(when(col("is_picc").equalTo("0") && col("is_fbj").equalTo("0"), col("order_amount")).otherwise(0)) as "nonPiccMarried")
      
      //将退货完成元数据地市替换为统计维度的地市
      val returnBaseDataDf = backwardDf.as("df1").join(broadcast(validCityDf).as("df2"),backwardDf("city") === validCityDf("city_code"),"inner")
        .select(
            col("df1.province"),
            col("df2.business_city_code") as "city",
            col("df1.manager_id"),
            col("df1.manager_name"),
            col("df1.is_picc"),
            col("df1.is_fbj"),
            col("df1.refund_amount"))
       
      //按省、市、客户经理维度聚合计算
      val backwardProvinceCityManagerDataDf = returnBaseDataDf.groupBy("province","city","manager_id","manager_name")
        .agg(
            sum(col("refund_amount")) as "returnB2bOrder",
            sum(when(col("is_picc").equalTo("1") && col("is_fbj").equalTo("1"), col("refund_amount")).otherwise(0)) as "returnPiccSelfop",
            sum(when(col("is_picc").equalTo("1") && col("is_fbj").equalTo("0"), col("refund_amount")).otherwise(0)) as "returnPiccMarried",
            sum(when(col("is_picc").equalTo("0") && col("is_fbj").equalTo("1"), col("refund_amount")).otherwise(0)) as "returnNonPiccSelfop",
            sum(when(col("is_picc").equalTo("0") && col("is_fbj").equalTo("0"), col("refund_amount")).otherwise(0)) as "returnNonPiccMarried")
      
      //正向确认收货口径数据
      val tempDeliverDf1 = forwardProvinceCityManagerDataDf.as("df1").join(broadcast(backwardProvinceCityManagerDataDf).as("df2"),Seq("province","city"),"left")
        .select(
            col("df1.province"),
            col("df1.city"),
            col("df1.manager_id"),
            col("df1.manager_name"),
            col("df1.b2bOrder") - when(col("df2.returnB2bOrder").isNull, 0).otherwise(col("df2.returnB2bOrder")) as "deliveredB2bOrder",
            col("df1.piccSelfop") - when(col("df2.returnPiccSelfop").isNull, 0).otherwise(col("df2.returnPiccSelfop")) as "deliveredPiccSelfop",
            col("df1.piccMarried") - when(col("df2.returnPiccMarried").isNull, 0).otherwise(col("df2.returnPiccMarried")) as "deliveredPiccMarried",
            col("df1.nonPiccSelfop") - when(col("df2.returnNonPiccSelfop").isNull, 0).otherwise(col("df2.returnNonPiccSelfop")) as "deliveredNonPiccSelfop",
            col("df1.nonPiccMarried") - when(col("df2.returnNonPiccMarried").isNull, 0).otherwise(col("df2.returnNonPiccMarried")) as "deliveredNonPiccMarried"
            )
      
      //逆向退货完成口径数据
      val tempDeliverDf2 = forwardProvinceCityManagerDataDf.as("df1").join(broadcast(backwardProvinceCityManagerDataDf).as("df2"),Seq("province","city"),"right")
        .select(
            col("df2.province"),
            col("df2.city"),
            col("df2.manager_id"),
            col("df2.manager_name"),
            when(col("df1.b2bOrder").isNull,0).otherwise(col("df1.b2bOrder")) - col("df2.returnB2bOrder") as "deliveredB2bOrder",
            when(col("df1.piccSelfop").isNull,0).otherwise(col("df1.piccSelfop")) - col("df2.returnPiccSelfop") as "deliveredPiccSelfop",
            when(col("df1.piccMarried").isNull,0).otherwise(col("df1.piccMarried")) - col("df2.returnPiccMarried") as "deliveredPiccMarried",
            when(col("df1.nonPiccSelfop").isNull,0).otherwise(col("df1.nonPiccSelfop")) - col("df2.returnNonPiccSelfop") as "deliveredNonPiccSelfop",
            when(col("df1.nonPiccMarried").isNull,0).otherwise(col("df1.nonPiccMarried")) - col("df2.returnNonPiccMarried") as "deliveredNonPiccMarried"
            )
      
      val deliveredDataDf = tempDeliverDf1.union(tempDeliverDf2).na.fill(0)
      
      //计算确认实收数据
      options += ("dbtable" -> "base_jap_order_transaction_track")
      options += ("partitionColumn" -> "id")
      options += ("lowerBound" -> "1")
      options += ("upperBound" -> config.getUpperBound(options).toString)
      options += ("numPartitions" -> "50")
      
      val actualBaseDataDf = spark.read.format("jdbc").options(options).load
        .na.drop(Array("deliver_date","operate_date"))
      
      val filteredActualB2bDataDf = actualBaseDataDf.as("df1").select(col("df1.city"),col("df1.order_id"),col("df1.deliver_date"),col("df1.operate_date"),col("df1.amount"))
        .filter(row => {
          var suchDate: String = ""
          val deliverDate = row.getAs[Date]("deliver_date")
          val operateDate = row.getAs[Date]("operate_date")
          if(deliverDate.getTime < operateDate.getTime)
            suchDate = operateDate.toString
          else
            suchDate = deliverDate.toString
          if(filterMap("date").size > 0 && !filterMap("date").contains(suchDate))
            false
          else
            true
        })
        
        //按订单号聚合计算实收金额
        val orderAmountDf = filteredActualB2bDataDf.as("df1").join(broadcast(validCityDf).as("df2"), filteredActualB2bDataDf("city") === validCityDf("city_code"),"inner")
        .groupBy(col("df1.order_id"))
        .agg(sum(col("df1.amount")) as "order_amount")
        
        val actualB2bBaseDataDf = baseB2bDataDf.as("df1").join(broadcast(orderAmountDf).as("df2"), Seq("order_id"),"inner")
          .select(
              col("df1.province"),
              col("df1.city"),
              col("df1.manager_id"),
              col("df1.manager_name"),
              col("df1.is_picc"),
              col("df1.is_fbj"),
              col("df2.order_amount"))
         
        //按省、市、客户经理聚合计算各项实收业务数据
        val actualDataDf = actualB2bBaseDataDf.groupBy("province","city","manager_id","manager_name")
        .agg(
            sum(col("order_amount")) as "actualB2bOrder",
            sum(when(col("is_picc").equalTo("1") && col("is_fbj").equalTo("1"), col("order_amount")).otherwise(0)) as "actualPiccSelfop",
            sum(when(col("is_picc").equalTo("1") && col("is_fbj").equalTo("0"), col("order_amount")).otherwise(0)) as "actualPiccMarried",
            sum(when(col("is_picc").equalTo("0") && col("is_fbj").equalTo("1"), col("order_amount")).otherwise(0)) as "actualNonPiccSelfop",
            sum(when(col("is_picc").equalTo("0") && col("is_fbj").equalTo("0"), col("order_amount")).otherwise(0)) as "actualNonPiccMarried")
       
        options += ("url" -> salesDb)
        options += ("dbtable" -> "manager_achievement_statistics_copy")
        val url = options("url"); val user = options("user"); val password = options("password"); val tableName = options("dbtable")
        //将确认收货数据插入数据库
        deliveredDataDf.foreach(row => {
          val province = row.getAs[String]("province")
          val city = row.getAs[String]("city")
          val managerId = row.getAs[Long]("manager_id")
          val managerName = row.getAs[String]("manager_name")
          val b2bOrder = row.getAs[java.math.BigDecimal]("deliveredB2bOrder")
          val piccSelfop = row.getAs[java.math.BigDecimal]("deliveredPiccSelfop")
          val piccMarried = row.getAs[java.math.BigDecimal]("deliveredPiccMarried")
          val nonPiccSelfop = row.getAs[java.math.BigDecimal]("deliveredNonPiccSelfop")
          val nonPiccMarried = row.getAs[java.math.BigDecimal]("deliveredNonPiccMarried")
          val dt = year + month + day
          
          val sql = (""
              + "INSERT INTO " + tableName + "(date,province,city,manager_id,manager_name,b2b_order,picc_selfop_order,picc_married_order,nonpicc_selfop_order,nonpicc_married_order,dt,create_time,update_time) "
              + "VALUES ('" + dt + "','" + province + "','" + city + "'," + managerId + ",'" + managerName + "',"
              + b2bOrder + "," + piccSelfop + "," + piccMarried + "," + nonPiccSelfop + "," + nonPiccMarried + ",'"
              + date + "',NOW(),NOW()) "
              + "ON DUPLICATE KEY UPDATE "
                + "b2b_order = " + b2bOrder + ", picc_selfop_order = " + piccSelfop + ", picc_married_order = " + piccMarried + ","
                + "nonpicc_selfop_order = " + nonPiccSelfop + ", nonpicc_married_order = " + nonPiccMarried + ", update_time = NOW()")
          insertIntoTable(url, user, password, sql)
        })
        
        //将确认实收数据插入数据库
        actualDataDf.foreach(row => {
          val province = row.getAs[String]("province")
          val city = row.getAs[String]("city")
          val managerId = row.getAs[Long]("manager_id")
          val managerName = row.getAs[String]("manager_name")
          val b2bOrder = row.getAs[java.math.BigDecimal]("actualB2bOrder")
          val piccSelfop = row.getAs[java.math.BigDecimal]("actualPiccSelfop")
          val piccMarried = row.getAs[java.math.BigDecimal]("actualPiccMarried")
          val nonPiccSelfop = row.getAs[java.math.BigDecimal]("actualNonPiccSelfop")
          val nonPiccMarried = row.getAs[java.math.BigDecimal]("actualNonPiccMarried")
          val dt = year + month + day
          
          val sql = (""
              + "INSERT INTO " + tableName + "(date,province,city,manager_id,manager_name,b2b_actual_order,picc_selfop_actual_order,picc_married_actual_order,nonpicc_selfop_actual_order,nonpicc_married_actual_order,dt,create_time,update_time) "
              + "VALUES ('" + dt + "','" + province + "','" + city + "'," + managerId + ",'" + managerName + "',"
              + b2bOrder + "," + piccSelfop + "," + piccMarried + "," + nonPiccSelfop + "," + nonPiccMarried + ",'"
              + date + "',NOW(),NOW()) "
              + "ON DUPLICATE KEY UPDATE "
                + "b2b_actual_order = " + b2bOrder + ", picc_selfop_actual_order = " + piccSelfop + ", picc_married_actual_order = " + piccMarried + ","
                + "nonpicc_selfop_actual_order = " + nonPiccSelfop + ", nonpicc_married_actual_order = " + nonPiccMarried + ", update_time = NOW()")
          insertIntoTable(url, user, password, sql)
        })
    }
    
    //B2b撮合服务费计算
    def marriedServiceCharge {
      val config = new Configuration
      options += ("url" -> jiaanpeiReportDb)
      options += ("dbtable" -> "jc_settlement_list")
      options += ("partitionColumn" -> "id")
      options += ("lowerBound" -> "1")
      options += ("upperBound" -> config.getUpperBound(options).toString)
      options += ("numPartitions" -> "50")
      
      val allDates = dates
      val date = allDates(0);val year = allDates(1); val month = allDates(2); val day = allDates(3)
      val filterMap = Map(
            "date" -> List(allDates(0)))
      
      val calendar = Calendar.getInstance
      calendar.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(date))
      calendar.add(Calendar.DAY_OF_MONTH, 1)
      val plusDate = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime)
      
      val baseServiceChargeDf = spark.read.format("jdbc").options(options).load
        .na.drop(Array("service_charge_date"))
        .filter(row => {
          val supplierId = row.getAs[Long]("supplier_id")
          val serviceChargeDate = row.getAs[Timestamp]("service_charge_date")
          if(supplierId == 7550)
            false
          else if(serviceChargeDate.getTime < new SimpleDateFormat("yyyy-MM-dd").parse(date).getTime
              || serviceChargeDate.getTime > new SimpleDateFormat("yyyy-MM-dd").parse(plusDate).getTime)
            false
          else
            true
        })
      
      options += ("dbtable" -> "base_jap_order")
      options += ("partitionColumn" -> "id")
      options += ("lowerBound" -> "1")
      options += ("upperBound" -> config.getUpperBound(options).toString)
      options += ("numPartitions" -> "50")
      val baseB2bOrderDf = spark.read.format("jdbc").options(options).load
      
      val baseServiceChargeOrderDf = baseB2bOrderDf.as("df1").join(broadcast(baseServiceChargeDf).as("df2"), Seq("order_id"), "inner")
        .select(
            col("df1.province"),
            col("df1.city"),
            col("df1.manager_id"),
            col("df1.manager_name"),
            col("df2.service_charge_amount") as "amount",
            when(col("df2.order_receive_time") < "2019-07-01 00:00:00",0).otherwise(col("df2.service_charge_amount")) as "bonus_amount"
            )
      
      //按省、市、客户经理聚合计算
      val serviceChargeProvinceCityDf = baseServiceChargeOrderDf.groupBy("province","city","manager_id","manager_name")
        .agg(
            round(sum(col("amount")) / 1.06 ,2) as "serviceCharge",
            round(sum(col("bonus_amount")) / 1.06, 2) as "bonusSerivceCharge"
            )
      
      //将数据插入数据库
      options += ("url" -> salesDb)
      options += ("dbtable" -> "manager_achievement_statistics_copy")
      val url = options("url"); val user = options("user"); val password = options("password"); val tableName = options("dbtable")
      
      serviceChargeProvinceCityDf.foreach(row => {
        val province = row.getAs[String]("province")
        val city = row.getAs[String]("city")
        val managerId = row.getAs[Long]("manager_id")
        val managerName = row.getAs[String]("manager_name")
        val serviceCharge = row.getAs[Double]("serviceCharge")
        val bonusServiceCharge = row.getAs[Double]("bonusSerivceCharge")
        val dt = year + month + day
        
        val sql = (""
              + "INSERT INTO " + tableName + "(date,province,city,manager_id,manager_name,actual_service_charge,bonus_service_charge,dt,create_time,update_time) "
              + "VALUES ('" + dt + "','" + province + "','" + city + "'," + managerId + ",'" + managerName + "',"
              + serviceCharge + "," + bonusServiceCharge + ",'"
              + date + "',NOW(),NOW()) "
              + "ON DUPLICATE KEY UPDATE "
                + "actual_service_charge = " + serviceCharge + ", bonus_service_charge = " + bonusServiceCharge + ", update_time = NOW()")
        insertIntoTable(url, user, password, sql)
      })
    }
}