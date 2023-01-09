package it.pps.ddos.utils

trait DataType[T]:
  /**
   * Type-level method for having a single instance of a value for all instances of a particular type T
   *
   * @return
   */
  def defaultValue: T

object DataType:
  def defaultValue[T](using data: DataType[T]): T = data.defaultValue

object GivenDataType:
  given IntDataType: DataType[Int] with
    override def defaultValue: Int = 0

  given DoubleDataType: DataType[Double] with
    override def defaultValue: Double = 0.0

  given BooleanDataType: DataType[Boolean] with
    override def defaultValue: Boolean = false

  given CharDataType: DataType[Char] with
    override def defaultValue: Char = ' '

  given StringDataType: DataType[String] with
    override def defaultValue: String = ""

  given AnyDataType: DataType[Any] with
    override def defaultValue: Any = None
