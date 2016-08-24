package com.xpandit.utils

import scala.tools.nsc.io.File

object CreateEvents {

  val NumEventsPerRack = 300
  val NumRacks = 250000
  val OutputPath = "src/main/resources/input/events.txt"

  def main(args: Array[String]): Unit = {

    val sb = new StringBuilder()

    for(i <- 0 until NumEventsPerRack){
      generateEvent(sb)
      println("Event " + (i+1) + " for all racks generated")
    }

    println("[Generated] " + NumEventsPerRack + " events x " + NumRacks + " racks = " + NumEventsPerRack * NumRacks)

  }


  def generateEvent(sb: StringBuilder): Unit = {

    for (rack <- 0 until NumRacks) {

      val temperature = (Math.random() * 10) + 31  //to get high temperature once in a while
      val event = rack + "|" + "%.2f".format(temperature)
      sb.append(event + "\n")
    }

    File(OutputPath).appendAll(sb.toString())
    sb.clear()
  }
}