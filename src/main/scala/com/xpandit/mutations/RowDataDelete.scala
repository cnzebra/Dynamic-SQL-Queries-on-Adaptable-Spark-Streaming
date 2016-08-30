package com.xpandit.mutations

/*
 * @author "Vasco Lopes" <vasco.lopes@xpand-it.com>
 */



class RowDataDelete(data: Array[String] = null, dataPKs: Array[String] = null, pos: Long) extends RowData(data, dataPKs) {

  override def getOperationType: String = OperationType.DELETE.toString

  override def getOperationPos = pos

}
