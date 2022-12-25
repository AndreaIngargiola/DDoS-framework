package it.pps.ddos.utils

trait DataType[T]:

  // it's defined as a type-level method beacause we only need one instance
  // of the value for all instances of a particular type T
  def defaultValue: T

given AnyDataType: DataType[Any] with
  override def defaultValue: Option[Any] = Option.empty

given IntDataType: DataType[Int] with
  override def defaultValue: Int = 0

given DoubleDataType: DataType[Double] with
  override def defaultValue: Double = 0.0

given BoolDataType: DataType[Boolean] with
  override def defaultValue: Boolean = false

given CharDataType: DataType[Char] with
  override def defaultValue: Char = ' '

given StringDataType: DataType[String] with
  override def defaultValue: String = ""