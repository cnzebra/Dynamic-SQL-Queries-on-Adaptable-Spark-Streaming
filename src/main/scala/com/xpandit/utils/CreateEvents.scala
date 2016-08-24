package com.xpandit.utils

import scala.tools.nsc.io.File

object CreateEvents {

  val NumEventsPerPatient = 3
  val NumPatients = 10
  val OutputPath = "src/main/resources/input/events.txt"

  def main(args: Array[String]): Unit = {

    val sb = new StringBuilder()

    for(i <- 0 until NumEventsPerPatient){
      generateEvent(sb)
      println("Event " + (i+1) + " for all racks generated")
    }

    println("[Generated] " + NumEventsPerPatient + " events x " + NumPatients + " racks = " + NumEventsPerPatient * NumPatients)

  }


  def generateEvent(sb: StringBuilder): Unit = {

    for (patient <- 0 until NumPatients) {

      val name = "Name" + patient
      val age = ((Math.random() * 20) + 10).asInstanceOf[Int]
      val receptionUrgency = (Math.random() * 10).asInstanceOf[Int]

      val event = patient + "|" + name + "|" + age + "|" + receptionUrgency + "|" + System.currentTimeMillis()
      println(event)
     // sb.append(event + "\n")
    }

    File(OutputPath).appendAll(sb.toString())
    sb.clear()
  }
}
