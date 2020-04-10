package com.spark.sql.business

/**
 * 每日数据统计
 */

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType,Decimal,DataType}
import java.sql.{Connection,Statement,SQLException,DriverManager,Date}
import org.apache.spark.sql.{Dataset,Row}

class BusinessDailyData(
    val sparkSession: SparkSession,
    val optionsMap: Map[String,String],
    val dates: List[String],
    val urls: List[String]) extends Serializable{
    @transient private val spark = sparkSession
    @transient private val options = optionsMap
    @transient private val date = dates
    @transient private val jiaanpeiReportDb = urls(0)
    @transient private val salesDb = urls(1)
    
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
    
    //
    //  每日业绩统计
    //
    def deliveredOrderData {
      val config = new Configuration()
      options += ("url" -> jiaanpeiReportDb)
      options += ("dbtable" -> "base_jap_order")
      options += ("partitionColumn" -> "id")
      options += ("lowerBound" -> "1")
      options += ("upperBound" -> config.getUpperBound(options).toString)
      options += ("numPartitions" -> "50")
      
      val allDates = this.date
      //过滤的内容
      val filterMap = Map(
            "date" -> List(allDates(0)),
            "b2b" -> List("1"))
            
      //基础数据
     val baseB2bDataDf = spark.read.format("jdbc").options(options).load
         .filter(row =>{
            if(filterMap("b2b").size > 0 && !filterMap("b2b").contains(row.getAs[String]("is_b2b")))
              false
            else
              true
          })
          
      //确认收货数据
      val cleanBaseB2bDataDf = baseB2bDataDf.na.drop(Array("deliver_date"))  //将确认收货日期为空的数据去除
      val forwardDf = cleanBaseB2bDataDf.filter(row =>{            
        val suchDate = row.getAs[Date]("deliver_date").toString
        if(filterMap("date").size > 0 && !filterMap("date").contains(suchDate))
          false
        else
          true
      })
      
      //退货完成数据
      val cleanBackwardBaseDataDf = baseB2bDataDf.na.drop(Array("refund_date"))  //将退货完成时间为空的数据删除
      val backwardDf = cleanBackwardBaseDataDf.filter(row =>{
        val suchDate = row.getAs[Date]("refund_date").toString
        if(filterMap("date").size > 0 && !filterMap("date").contains(suchDate))
          false
        else
          true
      })
      
      //支付完成数据
      val cleanBasePaidDataDf = baseB2bDataDf.na.drop(Array("payment_date"))  //将支付时间为空的数据去除
      val paymentDf = cleanBasePaidDataDf.filter(row =>{     
        val suchDate = row.getAs[Date]("payment_date").toString
        if(filterMap("date").size > 0 && !filterMap("date").contains(suchDate))
          false
        else
          true
      })
          
      //正向交易
     //计算各省市的正向B2b交易额
      val deliveredProvinceCityDf = forwardDf.groupBy("province", "city")
       .agg(count("id") as "noB2bOrder", 
            sum("order_amount") as "amountSalesOrder",
            sum("purchase_amount") as "amountPurchaseOrder",
            count(when(col("is_picc").equalTo("1"), col("id"))) as "noPiccOrder",
            sum(when(col("is_picc").equalTo("1"), col("order_amount")).otherwise(0)) as "amountPiccOrder",
            sum(when(col("is_picc").equalTo("1") && col("is_fbj").equalTo("1"),col("order_amount")).otherwise(0)) as "amountPiccSelfopOrder",
            sum(when(col("is_picc").equalTo("1") && col("is_fbj").equalTo("0"),col("order_amount")).otherwise(0)) as "amountPiccMarriedOrder",
            sum(when(col("is_picc").equalTo("0") && col("is_fbj").equalTo("1"),col("order_amount")).otherwise(0)) as "amountNonPiccSelfopOrder",
            sum(when(col("is_picc").equalTo("0") && col("is_fbj").equalTo("0"), col("order_amount")).otherwise(0)) as "amountNonPiccMarriedOrder",
            sum(when(col("is_picc").equalTo("1") && col("payment_type").equalTo("05"), col("order_amount")).otherwise(0)) as "amountPiccDirect",
            round(sum(when(col("is_fbj").equalTo("1"), col("order_amount")).otherwise(0)) / 1.13, 2) as "amountIncome"
            )
           
      //逆向
          
      val backwardProvinceCityDf = backwardDf.groupBy("province", "city")
        .agg(sum("refund_amount") as "amountRefundOrder",
            sum("refund_purchase_amount") as "amountRefundPurchaseOrder",
            sum(when(col("is_picc").equalTo("1"), col("refund_amount")).otherwise(0)) as "amountRefundPiccOrder",
            sum(when(col("is_picc").equalTo("1") && col("is_fbj").equalTo("1"),col("refund_amount")).otherwise(0)) as "amountRefundPiccSelfopOrder",
            sum(when(col("is_picc").equalTo("1") && col("is_fbj").equalTo("0"),col("refund_amount")).otherwise(0)) as "amountRefundPiccMarriedOrder",
            sum(when(col("is_picc").equalTo("0") && col("is_fbj").equalTo("1"),col("refund_amount")).otherwise(0)) as "amountRefundNonPiccSelfopOrder",
            sum(when(col("is_picc").equalTo("0") && col("is_fbj").equalTo("0"), col("refund_amount")).otherwise(0)) as "amountRefundNonPiccMarriedOrder",
            sum(when(col("is_picc").equalTo("1") && col("payment_type").equalTo("05"), col("refund_amount")).otherwise(0)) as "amountRefundPiccDirect",
            round(sum(when(col("is_fbj").equalTo("1"), col("order_amount")).otherwise(0)) / 1.13, 2) as "amountRefundIncome"
            )
            
      //支付口径
      val paidProvinceCityDataDf = paymentDf.groupBy("province", "city")
        .agg(count(col("id")) as "noPaidOrder",
            sum(col("order_amount")) as "amountPaidOrder",
            sum(when(col("is_picc").equalTo("1"), col("order_amount")).otherwise(0)) as "amountPaidPiccOrder")
      /**
       * 聚合计算确认收货口径的每日业绩
       */
      val deliverTempDataDf1 = deliveredProvinceCityDf.as("df1").join(broadcast(backwardProvinceCityDf).as("df2"),Seq("province","city"),"left")
        .select(
            col("df1.province"), 
            col("df1.city"),
            col("df1.noB2bOrder"),
            col("df1.noPiccOrder"),
            col("df1.amountSalesOrder") - when(col("df2.amountRefundOrder").isNull, 0).otherwise(col("df2.amountRefundOrder")) as "amountB2bOrder",
            col("df1.amountPurchaseOrder") - when(col("df2.amountRefundPurchaseOrder").isNull, 0).otherwise(col("df2.amountRefundPurchaseOrder")) as "amountPurchaseOrder",
            col("df1.amountPiccOrder") - when(col("df2.amountRefundPiccOrder").isNull, 0).otherwise(col("df2.amountRefundPiccOrder")) as "amountPiccOrder",
            col("df1.amountPiccSelfopOrder") - when(col("df2.amountRefundPiccSelfopOrder").isNull, 0).otherwise(col("df2.amountRefundPiccSelfopOrder")) as "amountPiccSelfopOrder",
            col("df1.amountPiccMarriedOrder") - when(col("df2.amountRefundPiccMarriedOrder").isNull, 0).otherwise(col("df2.amountRefundPiccMarriedOrder")) as "amountPiccMarriedOrder",
            col("df1.amountNonPiccSelfopOrder") - when(col("df2.amountRefundNonPiccSelfopOrder").isNull, 0).otherwise(col("df2.amountRefundNonPiccSelfopOrder")) as "amountNonPiccSelfopOrder",
            col("df1.amountNonPiccMarriedOrder") - when(col("df2.amountRefundNonPiccMarriedOrder").isNull, 0).otherwise(col("df2.amountRefundNonPiccMarriedOrder")) as "amountNonPicMarriedOrder",
            col("df1.amountPiccDirect") - when(col("df2.amountRefundPiccDirect").isNull, 0).otherwise(col("df2.amountRefundPiccDirect")) as "amountPiccDirect",
            col("df1.amountIncome") - when(col("df2.amountRefundIncome").isNull, 0).otherwise(col("df2.amountRefundIncome")) as "amountIncome"
            )
            
      val deliverTempDataDf2 = deliveredProvinceCityDf.as("df1").join(broadcast(backwardProvinceCityDf).as("df2"),Seq("province","city"),"right")
        .select(
            col("df2.province"), 
            col("df2.city"),
            when(col("df1.noB2bOrder").isNull,0) as "noB2bOrder",
            when(col("df1.noPiccOrder").isNull,0) as "noPiccOrder",
            when(col("df1.amountSalesOrder").isNull,0).otherwise(col("df1.amountSalesOrder")) - col("df2.amountRefundOrder") as "amountB2bOrder",
            when(col("df1.amountPurchaseOrder").isNull,0).otherwise(col("df1.amountPurchaseOrder")) - col("df2.amountRefundPurchaseOrder") as "amountPurchaseOrder",
            when(col("df1.amountPiccOrder").isNull,0).otherwise("df1.amountPiccOrder") - col("df2.amountRefundPiccOrder") as "amountPiccOrder",
            when(col("df1.amountPiccSelfopOrder").isNull,0).otherwise("df1.amountPiccSelfopOrder") - col("df2.amountRefundPiccSelfopOrder") as "amountPiccSelfopOrder",
            when(col("df1.amountPiccMarriedOrder").isNull,0).otherwise("df1.amountPiccMarriedOrder") - col("df2.amountRefundPiccMarriedOrder") as "amountPiccMarriedOrder",
            when(col("df1.amountNonPiccSelfopOrder").isNull,0).otherwise("df1.amountNonPiccSelfopOrder") - col("df2.amountRefundNonPiccSelfopOrder") as "amountNonPiccSelfopOrder",
            when(col("df1.amountNonPiccMarriedOrder").isNull,0).otherwise("df1.amountNonPiccSelfopOrder") - col("df2.amountRefundNonPiccMarriedOrder") as "amountNonPicMarriedOrder",
            when(col("df1.amountPiccDirect").isNull,0).otherwise("df1.amountPiccDirect") - col("df2.amountRefundPiccDirect") as "amountPiccDirect",
            when(col("df1.amountIncome").isNull,0).otherwise("df1.amountIncome") - col("df2.amountRefundIncome") as "amountIncome"
            )
      val deliveredOverallDataDf = deliverTempDataDf1.union(deliverTempDataDf2).na.fill(0)
      
      /**
       * 确认实收统计维度
       */
      options += ("dbtable" -> "base_jap_order_transaction_track")
      options += ("partitionColumn" -> "id")
      options += ("lowerBound" -> "1")
      options += ("upperBound" -> config.getUpperBound(options).toString)
      options += ("numPartitions" -> "50")
      
      val actualB2bDataDf = spark.read.format("jdbc").options(options).load
      val cleanBaseActualDataDf = actualB2bDataDf.na.drop(Array("operate_date","deliver_date"))  //将确认实收和确认收货时间为空的数据去除
      val actualBaseDataDf = cleanBaseActualDataDf.filter(row =>{  //选取需要的日期
          val operateDate = row.getAs[Date]("operate_date")
          val deliverDate = row.getAs[Date]("deliver_date")
          var suchDate: String = ""
          if(operateDate.getTime < deliverDate.getTime)
            suchDate = deliverDate.toString
          else
            suchDate = operateDate.toString
          if(filterMap("date").size > 0 && !filterMap("date").contains(suchDate))
            false
          else
            true
          })
      
      val joinedActualBaseDataDf = actualBaseDataDf.as("df1").join(baseB2bDataDf.as("df2"),actualBaseDataDf.col("iorder_id").equalTo(baseB2bDataDf.col("order_id")),"inner")
        .select(
            col("df1.id") as "id",
            col("df2.province") as "province", 
            col("df2.city") as "city",
            col("df2.is_fbj") as "is_fbj",
            col("df2.is_picc") as "is_picc",
            col("df2.order_id") as "order_id",
            col("df2.vehicle_type") as "vehicle_type",
            col("df2.payment_type") as "payment_type",
            col("df1.operate_code") as "operate_code",
            col("df2.deliver_date") as "deliver_date",
            col("df1.amount") as "order_amount",
            col("df2.purchase_amount") as "purchase_amount")
        .createTempView("actual_data_temp")
      
      //过滤确认收货时间在2020年1月1日之前的撮合账期订单
      val filteredActualBaseDataDf = spark.sql(""
         + "SELECT "
           + "id,province,city,is_fbj,is_picc,order_id,payment_type,vehicle_type,"
           + "operate_code,deliver_date,order_amount,purchase_amount "
        + "FROM actual_data_temp "
        + "WHERE CASE WHEN operate_code IN ('03','04') THEN deliver_date >= '2020-01-01' ELSE 1=1 END")
      
      val actualOverallDataDf = filteredActualBaseDataDf.groupBy("province", "city")
        .agg(countDistinct("order_id") as "noActualOrder", 
            sum("order_amount") as "amountActualB2bOrder",
            sum("purchase_amount") as "amountActualPurchaseOrder",
            count(when(col("is_picc").equalTo("1"), col("id"))) as "noActualPiccOrder",
            sum(when(col("is_picc").equalTo("1"), col("order_amount")).otherwise(0)) as "amountActualPiccOrder",
            sum(when(col("is_picc").equalTo("1") && col("is_fbj").equalTo("1"),col("order_amount")).otherwise(0)) as "amountActualPiccSelfopOrder",
            sum(when(col("is_picc").equalTo("1") && col("is_fbj").equalTo("0"),col("order_amount")).otherwise(0)) as "amountActualPiccMarriedOrder",
            sum(when(col("is_picc").equalTo("0") && col("is_fbj").equalTo("1"),col("order_amount")).otherwise(0)) as "amountActualNonPiccSelfopOrder",
            sum(when(col("is_picc").equalTo("0") && col("is_fbj").equalTo("0"), col("order_amount")).otherwise(0)) as "amountActualNonPiccMarriedOrder",
            sum(when(col("vehicle_type").equalTo("1"), col("order_amount")).otherwise(0)) as "amountActualPassenger",
            sum(when(col("vehicle_type").equalTo("0"), col("order_amount")).otherwise(0)) as "amountActualCommercial",            
            sum(when(col("is_picc").equalTo("1") && col("payment_type").equalTo("05"), col("order_amount")).otherwise(0)) as "amountActualPiccDirect",
            round(sum(when(col("is_fbj").equalTo("1"), col("order_amount")).otherwise(0)) / 1.13, 2) as "amountActualIncome"            
            )
       spark.catalog.dropTempView("actual_data_temp")
      
       /**
        * 将确认收货、确认实收、支付完成三种口径数据进行聚合    
        */
       val tempDataDf1 = deliveredOverallDataDf.join(actualOverallDataDf,Seq("province","city"),"left")
       val tempDataDf2 = deliveredOverallDataDf.join(actualOverallDataDf,Seq("province","city"),"right")
       val deliveredActualDataDf = tempDataDf1.union(tempDataDf2).na.fill(0)  //确认收货 + 确认实收数据
       
       val tempDataDf3 = deliveredActualDataDf.join(paidProvinceCityDataDf,Seq("province","city"),"left")
       val tempDataDf4 = deliveredActualDataDf.join(paidProvinceCityDataDf,Seq("province","city"),"right")
       val resultDf = tempDataDf3.union(tempDataDf4).na.fill(0)  //确认收货 + 确认实收 + 支付数据
       options += ("dbtable" -> "business_daily_data_copy")
       val year = allDates(1); val month = allDates(2); val day = allDates(3)
       val url = options("url"); val user = options("user"); val password = options("password"); val tableName = options("dbtable")
       resultDf.foreach(resultData => {
           val province = resultData.getAs[String]("province")
           val city = resultData.getAs[String]("city")
           val pid = province + city + year + month + day
           val yrMth = year + month
           val noB2bOrder = resultData.getAs[Long]("noB2bOrder")  //确认收货B2b订单量
           val noPiccOrder = resultData.getAs[Long]("noPiccOrder")  //确认收货人保订单量
           val amountB2bOrder = resultData.getAs[java.math.BigDecimal]("amountB2bOrder")  //确认收货B2b交易额
           val amountPurchaseOrder = resultData.getAs[java.math.BigDecimal]("amountPurchaseOrder")  //确认收货采购金额
           val amountPiccSelfopOrder = resultData.getAs[Double]("amountPiccSelfopOrder")  //确认收货人保自营交易额
           val amountPiccMarriedDeal = resultData.getAs[Double]("amountPiccMarriedOrder")  //确认收货人保撮合交易额
           val amountNonPiccSelfopOrder = resultData.getAs[Double]("amountNonPiccSelfopOrder")  //确认收货非人保自营交易额
           val amountNonPiccMarriedDeal = resultData.getAs[Double]("amountNonPicMarriedOrder")  //确认收货非人保撮合交易额
           val amountPiccDirectOrder = resultData.getAs[Double]("amountPiccDirect")  //确认收货保险直赔交易额
           val amountIncome = resultData.getAs[Double]("amountIncome")  //确认收货营收
           val noActualB2bOrder = resultData.getAs[Long]("noActualOrder")  //确认实收B2b订单量
           val noActualPiccOrder = resultData.getAs[Long]("noActualPiccOrder")  //确认实收人保订单量
           val amountActualB2bOrder = resultData.getAs[java.math.BigDecimal]("amountActualB2bOrder")  //确认实收B2b交易额
           val amountActualPurchaseOrder = resultData.getAs[java.math.BigDecimal]("amountActualPurchaseOrder")  //确认实收采购金额
           val amountAcutalPiccSelfopOrder = resultData.getAs[java.math.BigDecimal]("amountActualPiccSelfopOrder")  //确认实收人保自营交易额
           val amountActualPiccMarriedDeal = resultData.getAs[java.math.BigDecimal]("amountActualPiccMarriedOrder")  //确认实收人保撮合交易额
           val amountActualNonPiccSelfopOrder = resultData.getAs[java.math.BigDecimal]("amountActualNonPiccSelfopOrder")  //确认实收非人保自营交易额
           val amountActualNonPiccMarriedDeal = resultData.getAs[java.math.BigDecimal]("amountActualNonPiccSelfopOrder")  //确认实收非人保撮合交易额
           val amountActualPiccDirectOrder = resultData.getAs[java.math.BigDecimal]("amountActualPiccDirect")  //确认实收保险直赔交易额
           val amountActualPassenger = resultData.getAs[java.math.BigDecimal]("amountActualPassenger")  //确认实收乘用车交易额
           val amountActualCommercial = resultData.getAs[java.math.BigDecimal]("amountActualCommercial")  //确认实收乘用车交易额        
           val amountActualIncome = resultData.getAs[Double]("amountActualIncome")  //确认实收营收
           val noPaidOrder = resultData.getAs[Long]("noPaidOrder")  //支付订单量
           val amountPaidOrder = resultData.getAs[java.math.BigDecimal]("amountPaidOrder")  //支付订单金额
           val amountPaidPiccOrder = resultData.getAs[java.math.BigDecimal]("amountPaidPiccOrder")  //支付人保订单金额
           val sql = (""
                 + "INSERT INTO " + tableName + " (pid,province,city,"
                 + "no_b2b_order,no_picc_order,amount_b2b_order,amount_b2b_purchase_order,amount_picc_selfopOrder,amount_picc_marriedDeal,amount_nonpicc_selfopOrder,amount_nonpicc_marriedDeal,amount_picc_direct,amount_b2b_income,"
                 + "no_b2b_actual_order,no_picc_actual_order,amount_b2b_actual_order,amount_b2b_actual_purchaseOrder,amount_picc_actual_selfopOrder,amount_picc_actual_marriedDeal,amount_nonpicc_actual_selfopOrder,amount_nonpicc_actual_marriedDeal,amount_actual_picc_direct,amount_b2b_actual_income,"
                 + "no_paid_b2bOrder,amount_paid_b2bOrder,amount_paid_piccOrder,amount_passenger_order,amount_commercial_order,"
                 + "yr_mth,date,insert_time) "
                 + "VALUES ('" + pid + "','" + province + "','" + city + "',"
                           + noB2bOrder + "," + noPiccOrder + "," + amountB2bOrder + "," + amountPurchaseOrder + "," + amountPiccSelfopOrder + "," + amountPiccMarriedDeal + "," + amountNonPiccSelfopOrder + "," + amountNonPiccMarriedDeal + "," + amountPiccDirectOrder + "," + amountIncome + ","
                           + noActualB2bOrder + "," + noActualPiccOrder + "," + amountActualB2bOrder + "," + amountActualPurchaseOrder + "," + amountAcutalPiccSelfopOrder + "," + amountActualPiccMarriedDeal + "," + amountActualNonPiccSelfopOrder + "," + amountActualNonPiccMarriedDeal + "," + amountActualPiccDirectOrder + "," + amountActualIncome + ","
                           + noPaidOrder + "," + amountPaidOrder + "," + amountPaidPiccOrder + "," + amountActualPassenger + "," + amountActualCommercial + ",'"
                 + yrMth + "','" + allDates(0) + "',"
                 + "NOW()) "
                 + "ON DUPLICATE KEY UPDATE "
                   + "insert_time = NOW()"
                 )
          insertIntoTable(url, user, password, sql)
         })
      }
      
      //
      //  车型品牌数据
      //
      def vehiclePartOrderData {
        val allDates = this.date
        val config = new Configuration()
        options += ("url" -> jiaanpeiReportDb)
        options += ("dbtable" -> "base_jap_order")
        options += ("partitionColumn" -> "id")
        options += ("lowerBound" -> "1")
        options += ("upperBound" -> config.getUpperBound(options).toString)
        options += ("numPartitions" -> "50")
        
        val filterMap = Map(
              "date" -> List(allDates(0)),
              "b2b" -> List("1"))
          
        //获取昨日数据
        val baseB2bDataDf = spark.read.format("jdbc").options(options).load
            .filter(row =>{
              if(filterMap("b2b").size > 0 && !filterMap("b2b").contains(row.getAs[String]("is_b2b")))
                false
              else
                true
            })
            
        //确认收货数据
        val cleanBaseB2bDataDf = baseB2bDataDf.na.drop(Array("deliver_date"))  //将确认收货日期为空的数据去除
        val forwardDf = cleanBaseB2bDataDf.filter(row =>{            
          val suchDate = row.getAs[Date]("deliver_date").toString
          if(filterMap("date").size > 0 && !filterMap("date").contains(suchDate))
            false
          else
            true
         })
         
        //退货完成数据
        val cleanBackwardBaseDataDf = baseB2bDataDf.na.drop(Array("refund_date"))  //将退货完成时间为空的数据删除
        val backwardDf = cleanBackwardBaseDataDf.filter(row =>{
          val suchDate = row.getAs[Date]("refund_date").toString
          if(filterMap("date").size > 0 && !filterMap("date").contains(suchDate))
            false
          else
            true
        })
        
        //计算确认收货数据
        val deliveredProvinceCityDf = forwardDf.groupBy("province", "city")
         .agg(sum("oem_amount") as "amountOem",
            sum("order_amount") - sum("oem_amount") as "amountAfm",
            sum(when(col("vehicle_type").equalTo("1"), col("order_amount")).otherwise(0)) as "amountPassenger",
            sum(when(col("vehicle_type").equalTo("0"), col("order_amount")).otherwise(0)) as "amountCommercial",
            sum(when(col("vehicle_type").equalTo("9"), col("order_amount")).otherwise(0)) as "amountOther",
            sum(when(col("vehicle_type").equalTo("1"), col("oem_amount")).otherwise(0)) as "amountPassengerOem",
            sum(when(col("vehicle_type").equalTo("0"), col("oem_amount")).otherwise(0)) as "amountCommercialOem",
            sum(when(col("vehicle_type").equalTo("1"), col("order_amount") - col("oem_amount")).otherwise(0)) as "amountPassengerAfm",
            sum(when(col("vehicle_type").equalTo("0"), col("order_amount") - col("oem_amount")).otherwise(0)) as "amountCommercialAfm"
            )
        
        //计算退货完成数据
        val refundProvinceCityDf = backwardDf.groupBy("province", "city")
          .agg(sum("refund_oem_amount") as "amountRefundOem",
            sum("refund_amount") - sum("refund_oem_amount") as "amountRefundAfm",
            sum(when(col("vehicle_type").equalTo("1"), col("refund_amount")).otherwise(0)) as "amountRefundPassenger",
            sum(when(col("vehicle_type").equalTo("0"), col("refund_amount")).otherwise(0)) as "amountRefundCommercial",
            sum(when(col("vehicle_type").equalTo("9"), col("refund_amount")).otherwise(0)) as "amountRefundOther",
            sum(when(col("vehicle_type").equalTo("1"), col("refund_oem_amount")).otherwise(0)) as "amountRefundPassengerOem",
            sum(when(col("vehicle_type").equalTo("0"), col("refund_oem_amount")).otherwise(0)) as "amountRefundCommercialOem",
            sum(when(col("vehicle_type").equalTo("1"), col("refund_amount") - col("refund_oem_amount")).otherwise(0)) as "amountRefundPassengerAfm",
            sum(when(col("vehicle_type").equalTo("0"), col("refund_amount") - col("refund_oem_amount")).otherwise(0)) as "amountRefundCommercialAfm"
            )
            
        /**
         * 聚合计算确认收货
         */
        val deliveredTempDataDf1 = deliveredProvinceCityDf.as("df1").join(broadcast(refundProvinceCityDf).as("df2"), Seq("province","city"),"left")
          .select(
              col("df1.province"),
              col("df1.city"),
              col("df1.amountOem") - when(col("df2.amountRefundOem").isNull, 0).otherwise(col("df2.amountRefundOem")) as "amountOem",  
              col("df1.amountAfm") - when(col("df2.amountRefundAfm").isNull, 0).otherwise(col("df2.amountRefundAfm")) as "amountAfm",  
              col("df1.amountPassenger") - when(col("df2.amountRefundPassenger").isNull, 0).otherwise(col("df2.amountRefundPassenger")) as "amountPassenger",  
              col("df1.amountCommercial") - when(col("df2.amountRefundCommercial").isNull, 0).otherwise(col("df2.amountRefundCommercial")) as "amountCommercial",  
              col("df1.amountOther") - when(col("df2.amountRefundOther").isNull, 0).otherwise(col("df2.amountRefundOther")) as "amountOther", 
              col("df1.amountPassengerOem") - when(col("df2.amountRefundPassengerOem").isNull, 0).otherwise(col("df2.amountRefundPassengerOem")) as "amountPassengerOem",
              col("df1.amountCommercialOem") - when(col("df2.amountRefundCommercialOem").isNull, 0).otherwise(col("df2.amountRefundCommercialOem")) as "amountCommercialOem",
              col("df1.amountPassengerAfm") - when(col("df2.amountRefundPassengerAfm").isNull, 0).otherwise(col("df2.amountRefundPassengerAfm")) as "amountPassengerAfm",
              col("df1.amountCommercialAfm") - when(col("df2.amountRefundCommercialAfm").isNull, 0).otherwise(col("df2.amountRefundCommercialAfm")) as "amountCommercialAfm"
          )
          
        val deliveredTempDataDf2 = deliveredProvinceCityDf.as("df3").join(broadcast(refundProvinceCityDf).as("df4"), Seq("province","city"),"right")
          .select(
              col("df4.province"),
              col("df4.city"),
              col("df3.amountOem") - when(col("df4.amountRefundOem").isNull, 0).otherwise(col("df4.amountRefundOem")) as "amountOem",  
              col("df3.amountAfm") - when(col("df4.amountRefundAfm").isNull, 0).otherwise(col("df4.amountRefundAfm")) as "amountAfm",  
              col("df3.amountPassenger") - when(col("df4.amountRefundPassenger").isNull, 0).otherwise(col("df4.amountRefundPassenger")) as "amountPassenger",  
              col("df3.amountCommercial") - when(col("df4.amountRefundCommercial").isNull, 0).otherwise(col("df4.amountRefundCommercial")) as "amountCommercial",  
              col("df3.amountOther") - when(col("df4.amountRefundOther").isNull, 0).otherwise(col("df4.amountRefundOther")) as "amountOther", 
              col("df3.amountPassengerOem") - when(col("df4.amountRefundPassengerOem").isNull, 0).otherwise(col("df4.amountRefundPassengerOem")) as "amountPassengerOem",
              col("df3.amountCommercialOem") - when(col("df4.amountRefundCommercialOem").isNull, 0).otherwise(col("df4.amountRefundCommercialOem")) as "amountCommercialOem",
              col("df3.amountPassengerAfm") - when(col("df4.amountRefundPassengerAfm").isNull, 0).otherwise(col("df4.amountRefundPassengerAfm")) as "amountPassengerAfm",
              col("df3.amountCommercialAfm") - when(col("df4.amountRefundCommercialAfm").isNull, 0).otherwise(col("df4.amountRefundCommercialAfm")) as "amountCommercialAfm"
          )
        
        val deliveredOverAllDataDf = deliveredTempDataDf1.union(deliveredTempDataDf2).na.fill(0)
        
        
        /**
         * 支付完成但未确认收货
         */
        val paidDf = baseB2bDataDf.na.drop(Array("payment_date")).filter(row =>{
          val paymentDate = row.getAs[Date]("payment_date")
          val orderStatus = row.getAs[String]("order_status")
          if(paymentDate.getTime >= new SimpleDateFormat("yyyy-MM-dd").parse(allDates(1)+"-01-01").getTime 
              && paymentDate.getTime <= new SimpleDateFormat("yyyy-MM-dd").parse(allDates(0)).getTime
              && List("500","600").contains(orderStatus))
            true
          else
            false
        })
        
        val paidNotDeliveredDf = paidDf.groupBy("province", "city")
          .agg(
              sum("order_amount") as "amountPaidNotDelivered",
              sum(when(col("is_picc").equalTo("1") && col("is_fbj").equalTo("1"), "order_amount").otherwise(0)) as "piccSelfopNotDelivered",
              sum(when(col("is_picc").equalTo("1") && col("is_fbj").equalTo("0"), "order_amount").otherwise(0)) as "piccMarriedNotDelivered",
              sum(when(col("is_picc").equalTo("0") && col("is_fbj").equalTo("1"), "order_amount").otherwise(0)) as "NonpiccSelfopNotDelivered",
              sum(when(col("is_picc").equalTo("0") && col("is_fbj").equalTo("0"), "order_amount").otherwise(0)) as "NonpiccMarriedNotDelivered"              
              )
        
        /**
         * 聚合插入结果数据
         */
        val tempDataDf1 = deliveredOverAllDataDf.join(paidNotDeliveredDf,Seq("province","city"),"left")
        val tempDataDf2 = deliveredOverAllDataDf.join(paidNotDeliveredDf,Seq("province","city"),"right")
        val resultDf = tempDataDf1.union(tempDataDf2).na.fill(0)
        
        options += ("dbtable" -> "business_daily_complementary_data_copy")
        val year = allDates(1); val month = allDates(2); val day = allDates(3)
        val url = options("url"); val user = options("user"); val password = options("password"); val tableName = options("dbtable")
        
        resultDf.foreach(row => {
          val province = row.getAs[String]("province")
          val city = row.getAs[String]("city")
          val pid = province + city + year + month + day
          val yrMth = year + month
          val date = allDates(0)
          val oem = row.getAs[java.math.BigDecimal]("amountOem")  //原厂件金额
          val afm = row.getAs[java.math.BigDecimal]("amountAfm")  //品牌件金额
          val passenger = row.getAs[java.math.BigDecimal]("amountPassenger")  //乘用车金额
          val commercial = row.getAs[java.math.BigDecimal]("amountCommercial")  //商用车金额
          val other = row.getAs[java.math.BigDecimal]("amountOther")  //其他车型金额
          val passengerOem = row.getAs[java.math.BigDecimal]("amountPassengerOem")  //乘用车原厂件金额
          val commercialOem = row.getAs[java.math.BigDecimal]("amountCommercialOem")  //商用车原厂件金额
          val passengerAfm = row.getAs[java.math.BigDecimal]("amountPassengerAfm")  //乘用车品牌件金额
          val commercialAfm = row.getAs[java.math.BigDecimal]("amountCommercialAfm")  //商用车品牌件金额
          val paidNotDelivered = row.getAs[java.math.BigDecimal]("amountPaidNotDelivered")  //支付完成未确认收货金额
          val piccSelfopNotDelivered = row.getAs[Double]("piccSelfopNotDelivered")  //人保自营支付完成未确认收货金额
          val piccMarriedNotDelivered = row.getAs[Double]("piccMarriedNotDelivered")  //人保撮合支付完成未确认收货金额
          val nonpiccSelfopNotDelivered = row.getAs[Double]("NonpiccSelfopNotDelivered")  //非人保自营支付完成未确认收货金额
          val nonpiccMarriedNotDelivered = row.getAs[Double]("NonpiccMarriedNotDelivered")  //非人保撮合支付完成未确认收货金额 
          val sql = (""
                 + "INSERT INTO " + tableName + " (pid,province,city,"
                 + "amount_commercial_vehicle,amount_passenger_vehicle,amount_other_vehicle,"
                 + "amount_oem,amount_am,amount_commercial_vehicle_oem,amount_commercial_vehicle_am,"
                 + "amount_passenger_vehicle_oem,amount_passenger_vehicle_am,"
                 + "amount_paid_not_delivered,amount_picc_selfop_notdelivered,amount_picc_marriedDeal_notdelivered,"
                 + "amount_nonpicc_selfop_notdelivered,amount_nonpicc_marriedDeal_notdelivered,"
                 + "yr_mth,date,create_time,update_time) "
                 + "VALUES ('" + pid + "','" + province + "','" + city + "',"
                           + commercial + "," + passenger + "," + other + ","
                           + oem + "," + afm + "," + commercialOem + "," + commercialAfm + ","
                           + passengerOem + "," + passengerAfm + "," 
                           + paidNotDelivered + "," + piccSelfopNotDelivered + "," + piccMarriedNotDelivered + ","
                           + nonpiccSelfopNotDelivered + "," + nonpiccMarriedNotDelivered + ",'"
                 + yrMth + "','" + allDates(0) + "',"
                 + "NOW(),NOW()) "
                 + "ON DUPLICATE KEY UPDATE "
                   + "amount_commercial_vehicle = " + commercial + ", amount_passenger_vehicle = " + passenger + ", amount_other_vehicle = " + other + ","
                   + "amount_oem = " + oem + ", amount_am = " + afm + ", amount_commercial_vehicle_oem = " + commercialOem + ", amount_commercial_vehicle_am  = " + commercialAfm + ","
                   + "amount_passenger_vehicle_oem = " + passengerOem + ", amount_passenger_vehicle_am = " + passengerAfm + ","
                   + "amount_paid_not_delivered = " + paidNotDelivered + ", amount_picc_selfop_notdelivered = " + piccSelfopNotDelivered + ", amount_picc_marriedDeal_notdelivered = " + piccMarriedNotDelivered + ","
                   + "amount_nonpicc_selfop_notdelivered = " + nonpiccSelfopNotDelivered + ", amount_nonpicc_marriedDeal_notdelivered = " + nonpiccMarriedNotDelivered + ","
                   + "update_time = NOW()"
                 )
          insertIntoTable(url, user, password, sql)
        })
      }
      
      /**
       * 复购修理厂统计
       */
    def repeatPurchaseShop(price: Int) {
        val allDates = this.date
        val config = new Configuration()
        options += ("url" -> jiaanpeiReportDb)
        options += ("dbtable" -> "base_jap_order")
        options += ("partitionColumn" -> "id")
        options += ("lowerBound" -> "1")
        options += ("upperBound" -> config.getUpperBound(options).toString)
        options += ("numPartitions" -> "50")
        
        val filterMap = Map(
              "date" -> List(allDates(0)),
              "b2b" -> List("1"),
              "picc" -> List("1"))
              
        val date = allDates(0); val year = allDates(1); val month = allDates(2); val day = allDates(3)
        var url: String = ""; var user: String = ""; var password: String = ""; var tableName: String = ""
        
        val baseB2bDataDf = spark.read.format("jdbc").options(options).load
          .filter(row =>{
              if(filterMap("b2b").size > 0 && !filterMap("b2b").contains(row.getAs[String]("is_b2b")))
                false
              else if(filterMap("picc").size >0 && !filterMap("picc").contains(row.getAs[String]("is_picc")))
                false
              else
                true
            })
          .na.drop(Array("deliver_date"))
          
        //筛选出价格大于等于500，并且确认收货日期在当年1月1日至上月最后一天的订单
        //并去重取修理厂id
        var startDate: Date = null
        var endDate: Date = null
        if(month == "1")
          endDate = config.getDateValue(options, date, "thismonthlastday")
        else
          endDate = config.getDateValue(options, date, "lastmonthlastday")
        val baseRepeatShopIdDf = baseB2bDataDf.filter(row => {
          val orderAmount = row.getAs[java.math.BigDecimal]("order_amount").doubleValue  //订单金额
          val deliverDate = row.getAs[Date]("deliver_date")  //确认收货日期
          if(deliverDate.getTime >= new SimpleDateFormat("yyyy-MM-dd").parse(year+"-01-01").getTime
             && deliverDate.getTime <= endDate.getTime
             && orderAmount >= price)
            true
          else
            false
          }).select("repair_id").distinct

        options += ("dbtable" -> "jc_core_user")
        options += ("partitionColumn" -> "user_id")
        
        val coreUserDf = spark.read.format("jdbc").options(options).load
        val repairShopInfoDf = baseRepeatShopIdDf.as("df1")
          .join(broadcast(coreUserDf).as("df2"),baseRepeatShopIdDf("repair_id") === coreUserDf("user_id"),"inner")
          .select(
              col("df1.repair_id") as "repairId", 
              col("df2.company_name") as "name",
              col("df2.provinceCode") as "province",
              col("df2.cityCode") as "city")
        
        //将分母修理厂明细数据插入salesdb中修理厂明细表
        options += ("url" -> salesDb)
        options += ("dbtable" -> "data_repeat_shop_detail_copy")
        
        url = options("url"); user = options("user"); password = options("password"); tableName = options("dbtable")
        repairShopInfoDf.foreach(row => {
          val repairId = row.getAs[Long]("repairId")
          val repairName = row.getAs[String]("name")
          val province = row.getAs[String]("province")
          val city = row.getAs[String]("city")
          val sql = (""
              + "INSERT INTO " + tableName + "(shop_id,shop_name,province,city,year,month,type,create_time,update_time) "
              + "VALUES (" + repairId + ",'" + repairName + "','" + province + "','" + city + "','" + year + "','" + month.drop(1) + "','2',NOW(),NOW()) "
              + "ON DUPLICATE KEY UPDATE shop_id = " + repairId + "")          
          insertIntoTable(url, user, password, sql)
        })
        
        //根据分母修理厂明细数据按省份、地市计算分母修理厂数据
        val baseProvinceCityRepeatShopDf = repairShopInfoDf.groupBy("province", "city")
          .agg(count("repairId") as "noShop")
        
        //将上述计算的修理厂数据插入salesdb统计数据
        options += ("url" -> jiaanpeiReportDb)
        options += ("dbtable" -> "business_daily_complementary_data_copy")
        url = options("url"); user = options("user"); password = options("password"); tableName = options("dbtable")
        baseProvinceCityRepeatShopDf.foreach(row => {
         val province = row.getAs[String]("province")
         val city = row.getAs[String]("city")
         val noShop = row.getAs[Long]("noShop")
         val pid = province + city + year + month + day
         val yrMth = year + month
         
         val sql = (""
             + "INSERT INTO " + tableName + "(pid,province,city,repeat_purchase_shop_previous,date,yr_mth,create_time,update_time) "
             + "VALUES ('" + pid + "','" + province + "','" + city + "',"
             + noShop + ",'" + date + "','" + yrMth + "',NOW(),NOW()) "
             + "ON DUPLICATE KEY UPDATE repeat_purchase_shop_previous = " + noShop + ",update_time = NOW()") 
         insertIntoTable(url, user, password, sql)
       })
       
        //根据分母修理厂明细数据按省份计算
        val baseProvinceRepeatShopDf = repairShopInfoDf.groupBy("province")
          .agg(count("repairId") as "noShop")
       
        //插入salesdb中指定表
        options += ("url" -> salesDb)
        options += ("dbtable" -> "data_active_shop_copy")
        url = options("url"); user = options("user"); password = options("password"); tableName = options("dbtable")
        baseProvinceRepeatShopDf.foreach(row => {
          val province = row.getAs[String]("province")
          val noShop = row.getAs[Long]("noShop")
          val pid = province + date
         
          val sql = (""
             + "INSERT INTO " + tableName + "(pid,province,previous_repeat_shop,date,create_time,update_time) "
             + "VALUES ('" + pid + "','" + province + "',"
             + noShop + ",'" + date + "',NOW(),NOW()) "
             + "ON DUPLICATE KEY UPDATE previous_repeat_shop = " + noShop + ",update_time = NOW()")
          insertIntoTable(url, user, password, sql)
        })
          
        //计算本月产生人保交易，金额>=500
        startDate = config.getDateValue(options, date, "firstdayinmonth")
        println(startDate)
        val thisMonthValidShopDf = baseB2bDataDf.filter(row => {
          val orderAmount = row.getAs[java.math.BigDecimal]("order_amount").doubleValue
          val deliverDate = row.getAs[Date]("deliver_date")
          if(deliverDate.getTime >= startDate.getTime
              && deliverDate.getTime <= new SimpleDateFormat("yyyy-MM-dd").parse(date).getTime
              && orderAmount >= price)
            true
          else
            false
        })
        
        //JOIN分母修理厂明细
        val repeatShopInfoDf = repairShopInfoDf.as("df1")
          .join(broadcast(thisMonthValidShopDf).as("df2"),repairShopInfoDf("repairId") === thisMonthValidShopDf("repair_id"),"inner")
          .select(col("df2.repair_id"), col("df2.repair_name"),col("df1.province"),col("df1.city"))
        //选取关键字段插入复购修理厂明细表
        options += ("url" -> salesDb)
        options += ("dbtable" -> "data_repeat_shop_detail_copy")
        url = options("url"); user = options("user"); password = options("password"); tableName = options("dbtable")
        repeatShopInfoDf.foreach(row => {
          val repairId = row.getAs[Long]("repair_id")
          val repairName = row.getAs[String]("repair_name")
          val province = row.getAs[String]("province")
          val city = row.getAs[String]("city")
          
          val sql = (""
             + "INSERT INTO " + tableName + "(shop_id,shop_name,province,city,year,month,type,repeat_date,create_time,update_time) "
             + "VALUES (" + repairId + ",'" + repairName + "','" + province + "','" + city + "','"
             + year + "','" + month.drop(1) + "','1','" + date + "',NOW(),NOW()) "
             + "ON DUPLICATE KEY UPDATE shop_id = " + repairId + ",update_time = NOW()") 
          
          insertIntoTable(url, user, password, sql)
        })
        
        //聚合计算省、市维度复购修理厂数量，插入jiaanpei_report_db省市统计中
        val repeatShopProvinceCityDf = repeatShopInfoDf.groupBy("province", "city")
          .agg(countDistinct("repair_id") as "noShop")
        options += ("url" -> jiaanpeiReportDb)
        options += ("dbtable" -> "business_daily_complementary_data_copy")
        url = options("url"); user = options("user"); password = options("password"); tableName = options("dbtable")
        repeatShopProvinceCityDf.foreach(row => {
          val province = row.getAs[String]("province")
          val city = row.getAs[String]("city")
          val pid = province + city + year + month + day
          val noShop = row.getAs[Long]("noShop")
          val yrMth = year + month
          
          val sql = (""
             + "INSERT INTO " + tableName + "(pid,province,city,repeat_purchase_shop,date,yr_mth,create_time,update_time) "
             + "VALUES ('" + pid + "','" + province + "','" + city + "',"
             + noShop + ",'" + date + "','" + yrMth + "',NOW(),NOW())"
             + "ON DUPLICATE KEY UPDATE repeat_purchase_shop = " + noShop + ",update_time = NOW()") 
         
          insertIntoTable(url, user, password, sql)
        })
          
        //聚合省份计算复购修理厂数量，插入salesdb统计
        val repeatShopProvinceDf = repeatShopInfoDf.groupBy("province")
          .agg(countDistinct("repair_id") as "noShop")
        options += ("url" -> salesDb)
        options += ("dbtable" -> "data_active_shop_copy")
        url = options("url"); user = options("user"); password = options("password"); tableName = options("dbtable")
        repeatShopProvinceDf.foreach(row => {
          val province = row.getAs[String]("province")
          val noShop = row.getAs[Long]("noShop")
          val pid = province + date
          
          val sql = (""
             + "INSERT INTO " + tableName + "(pid,province,this_month_repeat_shop,date,create_time,update_time) "
             + "VALUES ('" + pid + "','" + province + "'," + noShop + ",'"
             + date + "',NOW(),NOW()) "
             + "ON DUPLICATE KEY UPDATE this_month_repeat_shop = " + noShop + ",update_time = NOW()") 
          
          insertIntoTable(url, user, password, sql)
        })
    }
}