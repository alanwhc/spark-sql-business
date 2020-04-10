package com.spark.sql.business
import scala.Tuple2
import org.apache.spark.sql.types.DoubleType

class BonusFactorFunction extends Serializable{
  //客户经理职级
  private def getCustomerManagerClass(orderAmount: Double) = {
    if(orderAmount.doubleValue <= 850000)
      "1"
    else if(orderAmount.doubleValue > 8500000 && orderAmount.doubleValue <= 1500000)
      "2"
    else if(orderAmount.doubleValue > 1500001 && orderAmount.doubleValue <= 3000000)
      "3"
    else if(orderAmount.doubleValue > 3000001 && orderAmount.doubleValue <= 6000000)
      "4"
    else
      "5"
  }
  
  //运营岗职级
  private def getOperateSupplychainClass(orderAmount: Double) = {
    if(orderAmount.doubleValue <= 10000000)
      "2"
    else if(orderAmount.doubleValue > 10000000 && orderAmount.doubleValue <= 20000000)
      "3"
    else if(orderAmount.doubleValue > 20000000 && orderAmount.doubleValue <= 30000000)
      "4"
    else
      "5"
  }
  
  //省级负责人职级
  private def getProvincialManagerClass(orderAmount: Double) = {
    if(orderAmount.doubleValue <= 6000000)
      "1.1"
    else if(orderAmount.doubleValue > 6000000 && orderAmount.doubleValue <= 10000000)
      "1.2"
    else if(orderAmount.doubleValue > 10000000 && orderAmount.doubleValue <= 15000000)
      "2.1"
    else if(orderAmount.doubleValue > 15000000 && orderAmount.doubleValue <= 20000000)
      "2.2"
    else
      "3"
  }
  
  //客户经理自营毛利提成比例
  private def getCustomerManagerSelfopProfitRatio(_class: String) = {
    if(_class == "1")
      0.07
    else if(_class == "2")
      0.11
    else if(_class == "3")
      0.15
    else if(_class == "4")
      0.17
    else
      0.18
  }
  
  //客户经理撮合毛利提成比例
  private def getCustomerManagerMarriedProfitRatio(_class: String) = {
    if(_class == "1")
      0.04
    else if(_class == "2")
      0.08
    else if(_class == "3")
      0.10
    else if(_class == "4")
      0.12
    else
      0.14
  }
  
  //省级运营岗毛利提成比例
  private def getOperateProfitRatio(_class: String) = {
    if(_class == "2")
      0.016
    else if(_class == "3")
      0.018
    else if(_class == "4")
      0.020
    else if(_class == "5")
      0.022
    else
      0
  }
  
  //区域供应链毛利提成比例
  private def getSupplychainProfitRatio(_class: String) = {
    if(_class == "2")
      0.014
    else if(_class == "3")
      0.015
    else if(_class == "4")
      0.016
    else if(_class == "5")
      0.017
    else
      0
  }
  
  //省级负责人毛利提成比例
  private def getProvincialProfitRatio(_class: String) = {
    if(_class == "1.1")
      0.025
    else if(_class == "1.2")
      0.028
    else if(_class == "2.1")
      0.032
    else if(_class == "2.2")
      0.035
    else
      0.038
  }
  
  //大区负责人毛利提成比例
  private def getRegionalProfitRatio = {
    0.012
  }
  
  /**
   * 人员职级
   */
  def getSalesClass:(String => String) = (posAmount: String) => {
    val position = posAmount.substring(0,2)
    val orderAmount = posAmount.substring(2).toDouble
    position match{
      case "01" => getProvincialManagerClass(orderAmount)
      case "02" => getOperateSupplychainClass(orderAmount)
      case "03" => getCustomerManagerClass(orderAmount)
      case "05" => getOperateSupplychainClass(orderAmount)
      case _ => "0"
    }
  }
  
  /**
   * 人员职级名称
   */
  def getSalesClassName:(String => String) = (posClass: String) => {
    val position = posClass.substring(0,2)
    val _class = posClass.substring(2)
    position match {
      case "00" => "M0"
      case "01" => "M" + _class.subSequence(0, 1).toString
      case "03" => "S" + _class
      case _ => "O" + _class
    }
  }
  
  /**
   * 大区负责人、省级负责人、运营岗、供应链毛利提成系数
   */
  def getBonusFactor:(String => Double) = (posClass: String) => {
    val position = posClass.substring(0, 2)
    val _class = posClass.substring(2)
    position match{
      case "00" => this.getRegionalProfitRatio
      case "01" => this.getProvincialProfitRatio(_class)
      case "02" => this.getOperateProfitRatio(_class)
      case "05" => this.getSupplychainProfitRatio(_class)
      case _ => 0
    }
  }
  
  /**
   * 客户经理自营实收毛利提成系数
   */
  def getManagerSelfopBonusFactor:(String => Double) = (_class: String) => {
    getCustomerManagerSelfopProfitRatio(_class)
  }
  
  /**
   * 客户经理撮合实收毛利提成系数
   */
  def getManagerMarriedBonusFactor:(String => Double) = (_class: String) => {
    getCustomerManagerMarriedProfitRatio(_class)
  }
  
  //人保自营完成率-自营提成比例
  def getCompletionSelfopRatio:(Double => Double) = (completion: Double) => {
    if(completion >= 110)
      1.1
    else
      1.0
  }
  
  //人保自营完成率-撮合提成比例
  def getCompletionMarriedRatio:(Double => Double) = (completion: Double) => {
    if(completion >= 100)
      1.0
    else
      max(completion / 100, 0.6)
  }
  
  /**
   * 修理厂复购率系数
   */
  def getRepeatShopFactor:(Double => Double) = (repeatRate: Double) => {
    if(repeatRate >= 40)
      1.0
    else if(repeatRate < 40 && repeatRate >= 35)
      0.9
    else if(repeatRate < 35 && repeatRate >= 25)
      0.8
    else
      0.7
  }
  
  private def max(a: Double, b: Double) = {
    if(a >= b) a
    else b
  }
}