package com.xpandit.mutations

/*
 * @author "Vasco Lopes" <vasco.lopes@xpand-it.com>
 */



class RowDataInsert(data: Array[String] = null, dataPKs: Array[String] = null, pos: Long) extends RowData(data, dataPKs) {

  override def getOperationType: String = OperationType.INSERT.toString

  override def getOperationPos = pos

}
