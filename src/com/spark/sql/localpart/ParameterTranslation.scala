package com.spark.sql.localpart

import scala.collection.mutable.ListBuffer

/**
 * 变量名称翻译
 */

class ParameterTranslation(
    val config: Configuration = null) extends Serializable {
  /**
   * 获取目标省份对应的表名
   */
  def getTargetProvinceTableName(variable: String, queryType: String) = {
    val tableName = "code_province_mapping"
    "picc_local_price_" + config.getBrandCodeProvinceName(tableName, variable, queryType)
  }
  
  /**
   * 获取标准品牌编码list
   */
  def getMatchingBrandCode(brands: Array[String], queryType: String) = {
    val tableName = "jy_brand_manufacturer_mapping"
    val brandCode = new ListBuffer[String]
    for(variable <- brands){
      val currBrandCode = config.getBrandCodeProvinceName(tableName, variable, queryType)
      brandCode.append(currBrandCode)
    }
    brandCode.toList
  }
  
  /**
   * 获取对比省份list
   */
  def getMatchingProvinceTableName(provinces: Array[String], queryType: String) = {
    val tableName = "code_province_mapping"
    val provinceList = new ListBuffer[String]
    for(variable <- provinces){
      val currProvince = config.getBrandCodeProvinceName(tableName, variable, queryType)
      provinceList.append("local_price_" + currProvince)
    }
    provinceList.toList
  }
  
  /**
   * 去除特殊字符
   */
   def removeSpecialCharacter:(String => String) = (character: String) => {
     character.replaceAll("""([+-/~.# ]|\\[s*])""", "")
   }
}