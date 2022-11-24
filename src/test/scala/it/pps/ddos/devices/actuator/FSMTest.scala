package it.pps.ddos.devices.actuator

import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.immutable.ListMap

class FSMTest extends AnyFlatSpec:
  "A Finite State Machine" should "be the result of the -- operator between an instance of State[T] and T" in testDSLPartialCreation()
  "The syntax 'A: State[T] -- t:T -> B: State[T]' " should "return a Finite State Machine that returns the state B given FSM(A, t)" in testDSLCreation()
  "The user" should "be able to reuse a state as many times as they want" in testReusableState()
  "The user" should not "be able to define more than one destination for a specific tuple of (State, T)" in testTupleOverride()
  "The result of the right-precedence _U operator" should "be the union of the two Finite State Machines" in testDSLUnion()

  private def testDSLPartialCreation(): Unit =
    A: State[String] = new State("A")
    assert(A--"transition" == new FSM[String](Option(A), Option("transition"), Option.empty))

  private def testDSLCreation(): Unit =
    A: State[String] = new State("A")
    B: State[String] = new State("B")
    fsm: FSM[String] = A -- "toB" -> B
    assert(fsm(A, "toB") == B)

  private def testReusableState(): Unit =
    A: State[String] = new State("A")
    B: State[String] = new State("B")
    fsm: FSM[String] = A -- "fromAtoB" -> B -- "fromBtoA" -> A -- "fromAtoA" -> A
    assert(fsm(A, "fromAtoA") == A && fsm(B, "fromBtoA") == A)

  private def testTupleOverride(): Unit =
    A: State[String] = new State("A")
    B: State[String] = new State("B")
    fsm: FSM[String] = A -- "overridedTransition" -> B -- "fromBtoA" -> A -- "overridedTransition" -> A
    asser(fsm(A, "overridedTransition") == A && fsm(A, "overridedTransition") != B)

  private def testDSLUnions(): Unit =
    A: State[String] = new State("A")
    B: State[String] = new State("B")
    partialFsm: FSM[String] = A -- "transition" -> B
    assert(partialFsm _U B -- "transition" -> A == A -- "transition" -> B -- "transition" -> A)