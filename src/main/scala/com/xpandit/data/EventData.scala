package com.xpandit.data

import com.xpandit.mutations.RowData


/**
  * @param columnIndex - map containing index in RowData's array for each column
  * @param columnType - map containing each column's type
  * @param rowData - RowData object containing each column value
  */
class EventData(val columnIndex: Map[String, Int], val columnType: Map[String, String], val rowData: RowData) extends Serializable{

  /**
    * Get string appended PrivateKey values
    * Used as key to partition events across workers
    */
  val getPrivateKeysValues : String = {
    var pksValues = new StringBuilder
    rowData.dataPKs.foreach( pk => pksValues.append(rowData.data(columnIndex.get(pk).get)))
    pksValues.toString()

  }

  def getAsString(column: String) : String = rowData.data(columnIndex(column))
  def getAsBoolean(column: String) : Boolean = rowData.data(columnIndex(column)).toBoolean
  def getAsByte(column: String) : Byte = rowData.data(columnIndex(column)).toByte
  def getAsShort(column: String) : Short = rowData.data(columnIndex(column)).toShort
  def getAsInt(column: String) : Int = rowData.data(columnIndex(column)).toInt
  def getAsLong(column: String) : Long = rowData.data(columnIndex(column)).toLong
  def getAsFloat(column: String) : Float = rowData.data(columnIndex(column)).toFloat
  def getAsDouble(column: String) : Double = rowData.data(columnIndex(column)).toDouble
  def getAsBigDecimal(column: String) : java.math.BigDecimal = new java.math.BigDecimal(rowData.data(columnIndex(column)))

  //TODO commplete with other availbale types
}
