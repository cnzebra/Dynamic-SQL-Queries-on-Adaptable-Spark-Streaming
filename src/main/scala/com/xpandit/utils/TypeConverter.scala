package com.xpandit.utils

/**
  * Created by xpandit on 8/30/16.
  */
object TypeConverter {

  def hiveToScalaStrType(typeStr: String) : String = {
    //TODO improve
    typeStr.toUpperCase match {
      case "STRING" => "String"
      case "BOOLEAN" => "Byte"
      case "TINYINT" => "String"
      case "SMALLINT" => "Short"
      case "INT" => "Int"
      case "BIGINT" => "Long"
      case "DOUBLE" => "Double"
      case "DECIMAL" => "BigDecimal"  //java.math
    }
  }
}
