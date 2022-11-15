package devices.sensors

/*
* Define logic sensors
* */
trait Sensor[A] {
  var internalStatus: A = _

  def processingFunction: A => A = ???
  def setStatus(phyInput: A): Unit =
    internalStatus = processingFunction(phyInput)
  def sendMessage(msg: GetStatus): Unit = ???
}

trait BasicSensor[A] {
  self: Sensor[A] =>
}

/*
* Actor of a basic sensor
* */
object SensorActor {
  enum Command:
    case GetStatus() extends Command
    case SetStatus[A](phyInput: A) extends Command

  def apply[A](sensor: Sensor[A]): Behavior[Command] =
    Behaviors.receiveMessage { message =>
      message match
        case GetStatus() =>
          println("Get message received. Getting status.. ")
          println("Sensor status: " + sensor.internalStatus)
          Behaviors.same
        case SetStatus(_) =>
          println("Set message received. Setting status.. ")
          sensor.setStatus(_)
          Behaviors.same
    }
}

/*
* Mixin example
* */
// class TemperatureSensor extends Sensor[Double] with BasicSensor[Double]