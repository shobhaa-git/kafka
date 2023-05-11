package kafka.server

import kafka.utils.Logging
import org.apache.kafka.server.{BrokerLogDirHealth, BrokerLogDirHealthChangeHandler}

import java.util.concurrent.ArrayBlockingQueue

class LogDirHealthChangeChannel(failureHandler: LogDirFailureChannel,
                                logDirNum: Int) extends BrokerLogDirHealthChangeHandler with Logging {

  private val healthChangeQueue = new ArrayBlockingQueue[String](logDirNum)

  override def onBrokerLogDirHealthChanged(logDirectory: String, health: BrokerLogDirHealth): Unit = {
    info(s"Processing health change notification to $health for log directory $logDirectory.")

    if (health == BrokerLogDirHealth.Slow || health == BrokerLogDirHealth.Stuck) {
      failureHandler.maybeAddOfflineLogDir(
        logDirectory,
        s"Log directory $logDirectory detected as $health",
        null)

    } else {
      healthChangeQueue.add(logDirectory)
    }
  }

  def takeNext(): String = healthChangeQueue.take()

}
