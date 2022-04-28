/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils

import kafka.log.Log
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time

import java.io.File
import java.util.concurrent.TimeUnit

final class BrokerLocalStorage(val brokerId: Int,
                               private val storageDirname: String,
                               private val storageWaitTimeoutSec: Int) {

  private val brokerStorageDirectory = new File(storageDirname)
  private val storagePollPeriodSec = 1
  private val time = Time.SYSTEM

  /**
    * Wait until the first segment offset in Apache Kafka storage for the given topic-partition is
    * equal or greater to the provided offset.
    *
    * This ensures segments can be retrieved from the local tiered storage when expected.
    */
  def waitForEarliestOffset(topicPartition: TopicPartition, offset: Long): Unit = {

    val timer = time.timer(TimeUnit.SECONDS.toMillis(storageWaitTimeoutSec))
    var earliestOffset = (0L, Seq[String]())

    while (timer.notExpired() && earliestOffset._1 < offset) {
      timer.sleep(TimeUnit.SECONDS.toMillis(storagePollPeriodSec))
      earliestOffset = getEarliestOffset(topicPartition)
    }

    val relativePos =
      if (earliestOffset._1 < offset) Some("smaller than")
      else if (earliestOffset._1 > offset)  Some("ahead of")
      else None

    relativePos.map { pos =>
      val message = s"[BrokerId=$brokerId] The base offset of the first log segment of $topicPartition in the log " +
        s"directory is ${earliestOffset._1} which is $pos the expected offset $offset. The directory of " +
        s"$topicPartition is made of the following files: \n${earliestOffset._2.mkString("\n")}"

      throw new AssertionError(message)
    }
  }

  def waitForAtLeastEarliestOffset(topicPartition: TopicPartition, offset: Long): Unit = {
    val timer = time.timer(TimeUnit.SECONDS.toMillis(storageWaitTimeoutSec))
    var earliestOffset = (0L, Seq[String]())

    while (timer.notExpired() && earliestOffset._1 < offset) {
      timer.sleep(TimeUnit.SECONDS.toMillis(storagePollPeriodSec))
      earliestOffset = getEarliestOffset(topicPartition)
    }

    val relativePos = if (earliestOffset._1 < offset) Some("smaller than") else None

    relativePos.map { pos =>
      val message = s"[BrokerId=$brokerId] The base offset of the first log segment of $topicPartition in the log " +
        s"directory is ${earliestOffset._1} which is $pos the expected offset $offset. The directory of " +
        s"$topicPartition is made of the following files: \n${earliestOffset._2.mkString("\n")}"

      throw new AssertionError(message)
    }
  }

  def eraseStorage(): Unit = {
    val (files, dirs) = brokerStorageDirectory.listFiles().partition(_.isFile)
    files.foreach(_.delete())
    dirs.flatMap((_: File).listFiles()).foreach(_.delete())
    dirs.foreach(_.delete())
  }

  private def getEarliestOffset(topicPartition: TopicPartition): (Long, Seq[String]) = {
    val topicPartitionFiles = getTopicPartitionFiles(topicPartition)

    val firstLogFile = topicPartitionFiles
      .filter(_.endsWith(Log.LogFileSuffix))
      .sorted
      .head

    (Log.offsetFromFileName(firstLogFile), topicPartitionFiles)
  }

  private def getTopicPartitionFiles(topicPartition: TopicPartition): Seq[String] = {
    val topicPartitionDir = brokerStorageDirectory
      .listFiles()
      .map(_.getName)
      .find(_ == topicPartition.toString)
      .getOrElse {
        throw new IllegalArgumentException(
          s"[BrokerId=$brokerId] Directory for the topic-partition $topicPartition was not found")
      }

    new File(brokerStorageDirectory, topicPartitionDir)
      .listFiles()
      .toIndexedSeq
      .map(_.getName())
  }

}
