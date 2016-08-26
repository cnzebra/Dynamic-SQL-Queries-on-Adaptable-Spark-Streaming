package com.xpandit.spark

/**
  *
  * @param healthServiceNumber  -
  * @param name - patient's name
  * @param age - patient's age
  * @param receptionUrgency - urgency in being checked by a doctor (0-10 scale)
  * @param requestTime - consulation request time
  */

@SerialVersionUID(100L)
class MedicalConsultationWaitingPacientEvent(val healthServiceNumber: Long, val name: String, val age: Int,
                                             val receptionUrgency: Int, val requestTime: Long) extends Serializable {

  val patientID = healthServiceNumber + name

  def canEqual(a: Any) = a.isInstanceOf[MedicalConsultationWaitingPacientEvent]

  override def equals(that: Any): Boolean = {
    that match {
      case that: MedicalConsultationWaitingPacientEvent => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + healthServiceNumber.hashCode()
    result = prime * result + name.hashCode()
    result = prime * result + age.hashCode()
    result = prime * result + receptionUrgency.hashCode()
    result = prime * result + requestTime.hashCode()
    return result
  }
}
