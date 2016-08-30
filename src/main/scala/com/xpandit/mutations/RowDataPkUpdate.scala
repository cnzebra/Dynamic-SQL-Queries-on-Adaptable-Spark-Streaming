package com.xpandit.mutations

/*
 * @author "Vasco Lopes" <vasco.lopes@xpand-it.com>
 */



class RowDataPkUpdate(data: Array[String] = null, dataPKs: Array[String] = null, val previousPKsData: Array[String] = null, pos: Long) extends RowDataUpdate(data, dataPKs, pos) {

  if (previousPKsData == null)
    throw new IllegalArgumentException("No previousPKsData provided in the constructor!")

  override def getOperationType: String = OperationType.PKUPDATE.toString

}