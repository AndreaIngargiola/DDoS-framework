package it.pps.ddos.utils

trait DataType[T]:
  def defaultValue: T

given DataType[Any] with
  override def defaultValue: Option[Any] = Option.empty

given DataType[Int] with
  override def defaultValue: Int = 0

given DataType[Double] with
  override def defaultValue: Double = 0.0

given DataType[Boolean] with
  override def defaultValue: Boolean = false

given DataType[Char] with
  override def defaultValue: Char = ' '

given DataType[String] with
  override def defaultValue: String = ""