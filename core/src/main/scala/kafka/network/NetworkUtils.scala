package kafka.network

/**
import kafka.server.KafkaConfig
import org.apache.kafka.clients.{ApiVersions, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ChannelBuilders, ListenerName, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{LogContext, Time}

import scala.jdk.CollectionConverters._

class NetworkUtils {

  private def buildNetworkClient(prefix: String,
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
}
**/
