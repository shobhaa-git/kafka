/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tiered.storage

import java.io.PrintStream
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import kafka.admin.AdminUtils.assignReplicasToBrokers
import kafka.admin.BrokerMetadata
import kafka.server.KafkaServer
import kafka.utils.BrokerLocalStorage
import kafka.utils.TestUtils
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.admin.{Admin, AdminClient}
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.server.log.remote.storage.{LocalTieredStorage, LocalTieredStorageHistory, LocalTieredStorageSnapshot}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.metadata.BrokerState

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.collection.{Seq, mutable}

final class TieredStorageTestContext(private val zookeeperClient: KafkaZkClient,
                                     private val brokers: Seq[KafkaServer],
                                     private val producerConfig: Properties,
                                     private val consumerConfig: Properties,
                                     private val securityProtocol: SecurityProtocol) {

  private[storage] val (ser, de) = (Serdes.String().serializer(), Serdes.String().deserializer())
  private val topicSpecs = mutable.Map[String, TopicSpec]()

  private val testReport = new TieredStorageTestReport(this)

  @volatile private var producer: KafkaProducer[String, String] = _
  @volatile private var consumer: KafkaConsumer[String, String] = _
  @volatile private var adminClient: Admin = _

  @volatile private var tieredStorages: Seq[LocalTieredStorage] = _
  @volatile private var localStorages: Seq[BrokerLocalStorage] = _

  initContext()

  def initContext(): Unit = {
    val bootstrapServerString = TestUtils.getBrokerListStrFromServers(brokers, securityProtocol)
    producerConfig.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServerString)
    consumerConfig.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServerString)

    //
    // Set a producer linger of 60 seconds, in order to optimistically generate batches of
    // records with a pre-determined size.
    //
    producerConfig.put(LINGER_MS_CONFIG, TimeUnit.SECONDS.toMillis(60).toString)

    val adminConfig = new Properties()
    adminConfig.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServerString)

    producer = new KafkaProducer[String, String](producerConfig, ser, ser)
    consumer = new KafkaConsumer[String, String](consumerConfig, de, de)
    adminClient = AdminClient.create(adminConfig)

    tieredStorages = TieredStorageTestHarness.getTieredStorages(brokers)
    localStorages = TieredStorageTestHarness.getLocalStorages(brokers)
  }

  def createTopic(spec: TopicSpec): Unit = {
    val assignments = spec.assignment.getOrElse {
      val metadata = brokers.head.metadataCache.getAliveBrokers.map(b => BrokerMetadata(b.id, Some(b.rack)))
      assignReplicasToBrokers(metadata, spec.partitionCount, spec.replicationFactor, 0, 0)
    }

    TestUtils.createTopic(zookeeperClient, spec.topicName, assignments, brokers, spec.properties)
    topicSpecs.synchronized { topicSpecs += spec.topicName -> spec }
  }

  def produce(records: Iterable[ProducerRecord[String, String]], batchSize: Int) = {
    //
    // Send the given records trying to honor the batch size. This is attempted
    // with a large producer linger and the use of an explicit flush every time
    // the number of a "group" of records reaches the batch size.
    // Note that "group" does not mean "batch"; the former is the result of a
    // user-driven behaviour, the latter is a construction internal to the
    // producer and part of the Kafka's ingestion mechanism.
    //
    records.grouped(batchSize).foreach { groupedRecords =>
      groupedRecords.foreach(producer.send(_))
      producer.flush()
    }
  }

  def consume(topicPartition: TopicPartition,
              numberOfRecords: Int,
              fetchOffset: Long = 0): Seq[ConsumerRecord[String, String]] = {

    consumer.assign(Seq(topicPartition).asJava)
    consumer.seek(topicPartition, fetchOffset)

    val records = new ArrayBuffer[ConsumerRecord[String, String]]
    def pollAction(polledRecords: ConsumerRecords[String, String]): Boolean = {
      records ++= polledRecords.asScala
      records.size >= numberOfRecords
    }

    val timeoutMs = 60000L
    val sep = System.lineSeparator()

    TestUtils.pollRecordsUntilTrue(consumer,
      pollAction,
      waitTimeMs = timeoutMs,
      msg = s"Could not consume $numberOfRecords records of $topicPartition from offset $fetchOffset " +
        s"in $timeoutMs ms. ${records.size} message(s) consumed:$sep${records.mkString(sep)}")

    records
  }

  def nextOffset(topicPartition: TopicPartition): Long = {
    consumer.assign(Seq(topicPartition).toList.asJava)
    consumer.seekToEnd(Seq(topicPartition).toList.asJava)
    consumer.position(topicPartition)
  }

  def bounce(brokerId: Int): Unit = {
    val broker = brokers(brokerId)

    closeClients()

    broker.shutdown()
    broker.awaitShutdown()
    broker.startup()

    initContext()
  }

  def stop(brokerId: Int): Unit = {
    val broker = brokers(brokerId)

    closeClients()

    broker.shutdown()
    broker.awaitShutdown()

    initContext()
  }

  def start(brokerId: Int): Unit = {
    val broker = brokers(brokerId)

    closeClients()

    broker.startup()

    initContext()
  }

  def eraseBrokerStorage(brokerId: Int): Unit = {
    localStorages(brokerId).eraseStorage()
  }

  def topicSpec(topicName: String) = topicSpecs.synchronized { topicSpecs(topicName) }

  def takeTieredStorageSnapshot(): LocalTieredStorageSnapshot = {
    LocalTieredStorageSnapshot.takeSnapshot(tieredStorages.head)
  }

  def getTieredStorageHistory(brokerId: Int): LocalTieredStorageHistory = tieredStorages(brokerId).getHistory

  def getTieredStorages: Seq[LocalTieredStorage] = tieredStorages

  def getLocalStorages: Seq[BrokerLocalStorage] = localStorages

  def admin() = adminClient

  def isActive(brokerId: Int): Boolean = {
    brokers(brokerId).brokerState.get() equals BrokerState.RUNNING
  }

  def isAssignedReplica(topicPartition: TopicPartition, replicaId: Int): Boolean = {
    val assignments = zookeeperClient.getPartitionAssignmentForTopics(Set(topicPartition.topic()))
    assignments(topicPartition.topic())(topicPartition.partition()).replicas.contains(replicaId)
  }

  def succeed(action: TieredStorageTestAction): Unit = {
    testReport.addSucceeded(action)
  }

  def fail(action: TieredStorageTestAction): Unit = {
    testReport.addFailed(action)
  }

  def printReport(output: PrintStream): Unit = {
    testReport.print(output)
  }

  def close(): Unit = {
    Utils.closeAll(producer, consumer)
    adminClient.close()
  }

  private def closeClients(): Unit = {
    producer.close(Duration.ofSeconds(5))
    consumer.close(Duration.ofSeconds(5))
    adminClient.close()
  }
}

final class TieredStorageTestReport(private val context: TieredStorageTestContext) {
  val successfulActions: mutable.Buffer[TieredStorageTestAction] = mutable.Buffer()
  val failedActions: mutable.Buffer[TieredStorageTestAction] = mutable.Buffer()

  def addSucceeded(action: TieredStorageTestAction): Unit = {
    this.synchronized { successfulActions += action }
  }

  def addFailed(action: TieredStorageTestAction): Unit = {
    this.synchronized { failedActions += action }
  }

  def print(output: PrintStream): Unit = {
    output.println()
    var seqNo = 0

    Seq(this.synchronized(successfulActions.toSeq), this.synchronized(failedActions.toSeq))
      .zip(Seq("SUCCESS", "FAILURE"))
      .foreach {
        case (actions, ident) => actions.foreach {
          case action =>
            seqNo += 1
            output.print(s"[${ident}] ($seqNo) ")
            action.describe(output)
            output.println()
          }
      }

    val lts = DumpLocalTieredStorage.dump(context.getTieredStorages.head, context.de, context.de)
    output.println(s"Content of local tiered storage:\n\n$lts")
  }
}
