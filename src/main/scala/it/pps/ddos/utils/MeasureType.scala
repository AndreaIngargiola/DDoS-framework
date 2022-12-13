package it.pps.ddos.utils

trait MeasureType

enum NumericType extends MeasureType:
  case IntType(value: Int = 0) extends NumericType
  case DoubleType(value: Double = 0.0) extends NumericType
  case BooleanType(value: Boolean = false) extends NumericType

enum TextType extends MeasureType:
  case CharType(value: Char = ' ') extends TextType
  case StringType(value: String = "") extends TextType

