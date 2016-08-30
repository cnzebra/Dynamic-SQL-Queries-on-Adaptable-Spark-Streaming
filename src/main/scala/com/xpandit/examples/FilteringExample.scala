package com.xpandit.examples

import com.xpandit.data.EventData
import com.xpandit.mutations.RowDataInsert
import com.xpandit.utils.SQLOnJavaCollections


/**
  * Created by xpandit on 8/29/16.
  */
object FilteringExample extends App{

  val userTypedWhereClause = "name = 'Andriy' AND age > 14"

  val rowData = new RowDataInsert(Array("Andriy", "21"), Array("pk"), 0)
  val columnIndex : Map[String, Int] = Map("name" -> 0, "age" -> 1)
  val columnType : Map[String, String] = Map("name" -> "String", "age" -> "Int")

  val eventData = new EventData(columnIndex, columnType, rowData)

  var listOfObjectsToFilter: java.util.List[EventData] = java.util.Arrays.asList(eventData)

  var replacedWhereClause = SQLOnJavaCollections.buildWhereClause(userTypedWhereClause, eventData)
  var query = SQLOnJavaCollections.buildQuery("com.xpandit.data.EventData", replacedWhereClause)

  var filteredList = SQLOnJavaCollections.apply(query, listOfObjectsToFilter)

  println("[User typed Where Clause] " + userTypedWhereClause)
  println("[Replaced Where Clause] " + replacedWhereClause)
  println(filteredList.toString)

}

