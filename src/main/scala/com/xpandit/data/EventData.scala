package com.xpandit.data

import com.xpandit.mutations.RowData


/**
  * @param columnIndex - map containing index in RowData's array for each column
  * @param columnType - map containing each column's type
  * @param rowData - RowData object containing each column value
  */
class EventData(val columnIndex: Map[String, Int], val columnType: Map[String, String], val rowData: RowData) extends Serializable{

  val getPrivateKeysValues : String = {
    var pksValues = new StringBuilder
    rowData.dataPKs.foreach( pk => pksValues.append(rowData.data(columnIndex.get(pk).get)))
    pksValues.toString()

  }

  def getAsString(column: String) : String = {
    rowData.data(columnIndex(column))
  }

  def getAsInt(column: String) : Int = {
    rowData.data(columnIndex(column)).toInt
  }

  //TODO complete with other available types, make sure those methods are called in FunctionHandlers class

}
