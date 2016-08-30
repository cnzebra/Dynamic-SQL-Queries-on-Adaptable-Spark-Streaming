package com.xpandit.mutations

/*
 * @author "Vasco Lopes" <vasco.lopes@xpand-it.com>
 */

object OperationType extends Enumeration {
  type OperationType = Value

  val INSERT = Value("I")
  val DELETE = Value("D")
  val UPDATE = Value("U")
  val PKUPDATE = Value("pkU")
  val PARTITIONUPDATE = Value("partitionU")
  val MIXUPDATE = Value("mixU")
}

