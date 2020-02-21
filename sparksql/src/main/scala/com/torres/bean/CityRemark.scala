package com.torres.bean

import java.text.DecimalFormat

case class CityRemark(name:String,ratio:Double){
  private val format: DecimalFormat = new DecimalFormat("0.00%")

  override def toString: String = s"$name:${format.format(ratio)}"
}
