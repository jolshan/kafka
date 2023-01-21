package kafka.server

import kafka.cluster.Partition
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse

import java.util.concurrent.locks.Lock
import scala.collection.Map

class DelayedTransactionCheck(delayMs: Long,
                              producerId: Long,
                              partition: Partition,
                              responseCallback: Map[TopicPartition, PartitionResponse] => Unit,
                              lockOpt: Option[Lock] = None)
  extends DelayedOperation(delayMs, lockOpt) {

  def onExpiration(): Unit = {
    // nothing yet
  }

  /**
   * Continue handling produce request via the callback.
   */
  def onComplete(): Unit = {
    responseCallback
  }

  /**
   * Check if we have a result on whether the transaction is ongoing.
   */
  def tryComplete(): Boolean = {
    partition.hasOngoingTransaction(producerId)
   }
}
