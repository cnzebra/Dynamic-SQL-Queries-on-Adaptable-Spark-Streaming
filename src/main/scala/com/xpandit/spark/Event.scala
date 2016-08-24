package com.xpandit.spark

import java.sql.Timestamp

import org.json.JSONObject

/**
  * Created by xpandit on 8/16/16.
  */

@SerialVersionUID(100L)
class Event(val time: Long, val key: String) extends Serializable {

  //TODO

  def canEqual(a: Any) = a.isInstanceOf[Event]

  override def equals(that: Any): Boolean = {
    that match {
      case that: Event => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + time.hashCode()
    return result
  }
}
