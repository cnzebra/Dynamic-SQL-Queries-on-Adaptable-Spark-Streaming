package com.xpandit.mutations

import com.xpandit.utils.Constants

/*
 * @author "Vasco Lopes" <vasco.lopes@xpand-it.com>
 *
 *
 * My changes:
 *    -added def getOperationPos: Long to RowData
 *
 *
 */


abstract class RowData(val data: Array[String] = null, val dataPKs: Array[String] = null) extends Serializable {

  if (data == null || dataPKs == null)
    throw new IllegalArgumentException("No data or no PKs provided in the constructor!")

  def this(numAttributes: Int) {

    this(new Array[String](numAttributes))

    for (i <- data.indices) {
      data(i) = ""
    }

  }

  override def toString: String = {
    val attribute = data.mkString(Constants.rowDataDelimiter)
    attribute + Constants.rowDataDelimiter + getOperationType
  }

  def getOperationType: String

  def getOperationPos: Long

  def getFormattedPKs: String = {
    dataPKs.mkString(Constants.rowDataDelimiter)
  }

}
