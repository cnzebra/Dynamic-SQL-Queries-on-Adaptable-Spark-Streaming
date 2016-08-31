package com.xpandit.utils

import org.apache.spark.sql.types._

/**
  * Created by xpandit on 8/30/16.
  */
object TypeConverter {


  def hiveToScalaStringType(typeStr: String) : String = {
    typeStr.toUpperCase match {
      case "STRING" => "STRING"
      case "BOOLEAN" => "BOOLEAN"
      case "TINYINT" => "BYTE"
      case "SMALLINT" => "SHORT"
      case "INT" => "INT"
      case "BIGINT" => "LONG"
      case "FLOAT" => "FLOAT"
      case "DOUBLE" => "DOUBLE"
      case "DECIMAL" => "BIGDECIMAL"
      case otherType => throw new RuntimeException("Data type not supported: " + otherType)
    }
  }

  def scalaToSparkSQLType(typeStr: String) : DataType = {
    typeStr.toUpperCase match {
      case "STRING" => StringType
      case "BOOLEAN" => BooleanType
      case "BYTE" => ByteType
      case "SHORT" => ShortType
      case "INT" => IntegerType
      case "LONG" => LongType
      case "FLOAT" => FloatType
      case "DOUBLE" => DoubleType
      case t if t.startsWith("BIGDECIMAL") =>
        val sqlTypeDecimal = typeStr.split("DECIMAL")
        val digits: Array[String] = sqlTypeDecimal(1).replace("(", "").replace(")", "").split(',')
        DecimalType(digits(0).toInt, digits(1).toInt)
    }
  }

  /**
    * Creates StructType used as schema to create dataframe from RDD[Row]
    */
  def generateStructType(tableColumnNamesArray: Array[String], columnScalaType: Map[String, String]) : StructType = {

    new StructType(tableColumnNamesArray.map{ (columnName) =>
      val sparkSQLType = scalaToSparkSQLType(columnScalaType.get(columnName).get)
      new StructField(columnName, sparkSQLType)
    })
  }


  /**
    * Converts given value in string to corresponding object
    */
  def castStrValueToObject(valueStr: String, typeStr: String) : Any  = {

      hiveToScalaStringType(typeStr) match {
      case "STRING" => valueStr
      case "BOOLEAN" => valueStr.toBoolean
      case "BYTE" => valueStr.toByte
      case "SHORT" => valueStr.toShort
      case "INT" => valueStr.toInt
      case "LONG" => valueStr.toLong
      case "FLOAT" => valueStr.toFloat
      case "DOUBLE" => valueStr.toDouble
      case t if t.startsWith("BIGDECIMAL") => new java.math.BigDecimal(valueStr)
    }
  }
}
