package kafka.network
import kafka.common.{InterBrokerSendThread, RequestAndCompletionHandler}
import kafka.server.{InterBrokerQueueItem, InterBrokerRequestCompletionHandler, KafkaConfig}
import org.apache.kafka.clients.{ApiVersions, ClientResponse, KafkaClient, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.Node
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ChannelBuilders, ListenerName, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{LogContext, Time}

import java.util.concurrent.LinkedBlockingDeque
import scala.jdk.CollectionConverters._

object NetworkUtils {

  def buildNetworkClient(prefix: String,
                                 config: KafkaConfig,
                                 metrics: Metrics,
                                 time: Time,
                                 logContext: LogContext,
                                 apiVersions: ApiVersions = new ApiVersions()): NetworkClient = {
    val controllerListenerName = new ListenerName(config.controllerListenerNames.head)
    val controllerSecurityProtocol = config.effectiveListenerSecurityProtocolMap.getOrElse(controllerListenerName, SecurityProtocol.forName(controllerListenerName.value()))
    val channelBuilder = ChannelBuilders.clientChannelBuilder(
      controllerSecurityProtocol,
      JaasContext.Type.SERVER,
      config,
      controllerListenerName,
      config.saslMechanismControllerProtocol,
      time,
      config.saslInterBrokerHandshakeRequestEnable,
      logContext
    )

    val metricGroupPrefix = prefix + "-channel"
    val collectPerConnectionMetrics = false

    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      config.connectionsMaxIdleMs,
      metrics,
      time,
      metricGroupPrefix,
      Map.empty[String, String].asJava,
      collectPerConnectionMetrics,
      channelBuilder,
      logContext
    )

    val clientId = prefix + s"-client-${config.nodeId}"
    val maxInflightRequestsPerConnection = 1
    val reconnectBackoffMs = 50
    val reconnectBackoffMsMs = 500
    val discoverBrokerVersions = true

    new NetworkClient(
      selector,
      new ManualMetadataUpdater(),
      clientId,
      maxInflightRequestsPerConnection,
      reconnectBackoffMs,
      reconnectBackoffMsMs,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      config.socketReceiveBufferBytes,
      config.quorumRequestTimeoutMs,
      config.connectionSetupTimeoutMs,
      config.connectionSetupTimeoutMaxMs,
      time,
      discoverBrokerVersions,
      apiVersions,
      logContext
    )
  }

  def buildRequestThread(
    name: String,
    networkClient: KafkaClient,
    requestTimeoutMs: Int,
    time: Time,
    isInterruptible: Boolean = true,
    retryTimeoutMs: Int = 60000
  ) = new GenericSendThread(name, networkClient, requestTimeoutMs, time, isInterruptible, retryTimeoutMs)
  class GenericSendThread(
    name: String,
    client: KafkaClient,
    requestTimeoutMs: Int,
    time: Time,
    isInterruptible: Boolean = true,
    retryTimeoutMs: Int = 60000
  ) extends InterBrokerSendThread(
    name,
    client,
    requestTimeoutMs,
    time,
    isInterruptible
  ) {
    private val queue = new LinkedBlockingDeque[InterBrokerQueueItem]()

    def receivingNode(): Option[Node] = {
      None
    }

    def updateReceivingNode(newActiveController: Node): Unit = {

    }

    def generateRequests(): Iterable[RequestAndCompletionHandler] = {
      val currentTimeMs = time.milliseconds()
      val requestIter = queue.iterator()
      while (requestIter.hasNext) {
        val request = requestIter.next
        if (currentTimeMs - request.createdTimeMs >= retryTimeoutMs) {
          requestIter.remove()
          request.callback.onTimeout()
        } else {
          val controllerAddress = receivingNode()
          if (controllerAddress.isDefined) {
            requestIter.remove()
            return Some(RequestAndCompletionHandler(
              time.milliseconds(),
              controllerAddress.get,
              request.request,
              handleResponse(request)
            ))
          }
        }
      }
      None
    }

    // REDO this
    private def handleResponse(queueItem: InterBrokerQueueItem)(response: ClientResponse): Unit = {
      if (response.authenticationException != null) {
        error(s"Request ${queueItem.request} failed due to authentication error with controller",
          response.authenticationException)
        queueItem.callback.onComplete(response)
      } else if (response.versionMismatch != null) {
        error(s"Request ${queueItem.request} failed due to unsupported version error",
          response.versionMismatch)
        queueItem.callback.onComplete(response)
      } else if (response.wasDisconnected()) {
        updateReceivingNode(null)
        queue.putFirst(queueItem)
      } else if (response.responseBody().errorCounts().containsKey(Errors.NOT_CONTROLLER)) {
        // just close the controller connection and wait for metadata cache update in doWork
        receivingNode().foreach { controllerAddress =>
          client.disconnect(controllerAddress.idString)
          updateReceivingNode(null)
        }

        queue.putFirst(queueItem)
      } else {
        queueItem.callback.onComplete(response)
      }
    }

    def sendRequest(
      request: AbstractRequest.Builder[_ <: AbstractRequest],
      callback: InterBrokerRequestCompletionHandler
    ): Unit = {
      queue.enqueue(InterBrokerQueueItem(
        time.milliseconds(),
        request,
        callback
      ))
    }
  }

}
