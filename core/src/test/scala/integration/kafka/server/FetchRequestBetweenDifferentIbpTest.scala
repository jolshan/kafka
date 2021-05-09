/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package integration.kafka.server

import java.time.Duration
import java.util.Arrays.asList

import kafka.api.{ApiVersion, KAFKA_2_7_IV0, KAFKA_2_8_IV1, KAFKA_3_0_IV0}
import kafka.server.{BaseRequestTest, KafkaConfig, KafkaServer}
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.collection.{Map, Seq, mutable}

class FetchRequestBetweenDifferentIbpTest extends BaseRequestTest {

  override def brokerCount: Int = 3
  override def generateConfigs: Seq[KafkaConfig] = {
    // Brokers should be at most 2 different IBP versions, but for more test coverage, three are used here.
    Seq(
      createConfig(0, KAFKA_2_7_IV0),
      createConfig(1, KAFKA_2_8_IV1),
      createConfig(2, KAFKA_3_0_IV0)
    )
  }

  @Test
  def testControllerOldIBP(): Unit = {
    val topic = "topic"
    val producer = createProducer()
    val consumer = createConsumer()

    // Ensure controller version < KAFKA_2_8_IV1, and then create a topic where leader of partition 0 is not the controller,
    // leader of partition 1 is.
    ensureControllerNotIn(servers.filter(_.config.interBrokerProtocolVersion != KAFKA_2_7_IV0))
    assertEquals(0, controllerSocketServer.config.brokerId)
    val partitionLeaders = createTopic(topic,  Map(0 -> Seq(1, 0, 2), 1 -> Seq(0, 2, 1)))
    TestUtils.waitForAllPartitionsMetadata(servers, topic, 2)

    assertEquals(1, partitionLeaders(0))
    assertEquals(0, partitionLeaders(1))

    val record1 = new ProducerRecord(topic, 0, null, "key".getBytes, "value".getBytes)
    val record2 = new ProducerRecord(topic, 1, null, "key".getBytes, "value".getBytes)
    producer.send(record1)
    producer.send(record2)

    consumer.assign(asList(new TopicPartition(topic, 0), new TopicPartition(topic, 1)))
    val count = consumer.poll(Duration.ofMillis(10000)).count() + consumer.poll(Duration.ofMillis(5000)).count()
    assertEquals(2, count)
  }

  @Test
  def testControllerNewIBP(): Unit = {
    val topic = "topic"
    val producer = createProducer()
    val consumer = createConsumer()

    // Ensure controller version = KAFKA_3_0_IV0, and then create a topic where leader of partition 1 is the old version.
    ensureControllerNotIn(servers.filter(_.config.interBrokerProtocolVersion != KAFKA_3_0_IV0))
    assertEquals(2, controllerSocketServer.config.brokerId)
    val partitionLeaders = createTopic(topic,  Map(0 -> Seq(1, 0, 2), 1 -> Seq(0, 2, 1)))
    TestUtils.waitForAllPartitionsMetadata(servers, topic, 2)

    assertEquals(1, partitionLeaders(0))
    assertEquals(0, partitionLeaders(1))

    val record1 = new ProducerRecord(topic, 0, null, "key".getBytes, "value".getBytes)
    val record2 = new ProducerRecord(topic, 1, null, "key".getBytes, "value".getBytes)
    producer.send(record1)
    producer.send(record2)

    consumer.assign(asList(new TopicPartition(topic, 0), new TopicPartition(topic, 1)))

    val count = consumer.poll(Duration.ofMillis(5000)).count() + consumer.poll(Duration.ofMillis(5000)).count()
    assertEquals(2, count)
  }

  @Test
  def testControllerNewToOldIBP(): Unit = {
    val topic = "topic"
    val topic2 = "topic2"
    val producer = createProducer()
    val consumer = createConsumer()

    // Ensure controller version = KAFKA_3_0_IV0
    ensureControllerNotIn(servers.filter(_.config.interBrokerProtocolVersion != KAFKA_3_0_IV0))
    assertEquals(2, controllerSocketServer.config.brokerId)
    val partitionLeaders = createTopic(topic,  Map(0 -> Seq(1, 0, 2), 1 -> Seq(0, 2, 1)))
    TestUtils.waitForAllPartitionsMetadata(servers, topic, 2)
    assertEquals(1, partitionLeaders(0))
    assertEquals(0, partitionLeaders(1))

    val record1 = new ProducerRecord(topic, 0, null, "key".getBytes, "value".getBytes)
    val record2 = new ProducerRecord(topic, 1, null, "key".getBytes, "value".getBytes)
    producer.send(record1)
    producer.send(record2)

    consumer.assign(asList(new TopicPartition(topic, 0), new TopicPartition(topic, 1)))

    val count = consumer.poll(Duration.ofMillis(5000)).count() + consumer.poll(Duration.ofMillis(5000)).count()
    assertEquals(2, count)

    // Make the broker whose version < KAFKA_2_8_IV0 controller
    ensureControllerNotIn(servers.filter(_.config.interBrokerProtocolVersion != KAFKA_2_7_IV0))
    assertEquals(0, controllerSocketServer.config.brokerId)
    createTopic(topic2,  Map(0 -> Seq(1, 0, 2)));
    TestUtils.waitForAllPartitionsMetadata(servers, topic2, 1)

    val record3 = new ProducerRecord(topic2, 0, null, "key".getBytes, "value".getBytes)
    val record4 = new ProducerRecord(topic, 1, null, "key".getBytes, "value".getBytes)
    producer.send(record3)
    producer.send(record4)

    consumer.assign(asList(new TopicPartition(topic, 0), new TopicPartition(topic, 1), new TopicPartition(topic2, 0)))

    val count2 = consumer.poll(Duration.ofMillis(5000)).count() + consumer.poll(Duration.ofMillis(5000)).count()
    assertEquals(2, count2)
  }

  //private def ensureControllerIn(brokerIds: Seq[Int]): Unit = {
  //  TestUtils.waitUntilControllerElected(zkClient)
  //  while (!brokerIds.contains(controllerSocketServer.config.brokerId)) {
  //    zkClient.deleteController(ZkVersion.MatchAnyVersion)
  //    TestUtils.waitUntilControllerElected(zkClient)
  //  }
  //}

  private def ensureControllerNotIn(nonControllerServers: mutable.Buffer[KafkaServer]): Unit = {
    nonControllerServers.iterator.foreach(server => {
      server.shutdown()
    })
    TestUtils.waitUntilControllerElected(zkClient)
    nonControllerServers.iterator.foreach(server => {
      server.startup()
    })
  }

  private def createConfig(nodeId: Int,interBrokerVersion: ApiVersion): KafkaConfig = {
    val props = TestUtils.createBrokerConfig(nodeId, zkConnect)
    props.put(KafkaConfig.InterBrokerProtocolVersionProp, interBrokerVersion.version)
    props.put(KafkaConfig.LogMessageFormatVersionProp, interBrokerVersion.version)
    KafkaConfig.fromProps(props)
  }

}