/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.index.spark

import org.apache.iceberg.FileScanTask
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * One index file per partition, each preferring the executors that already hold it.
 *
 * A partition carries the file to read. What it prefers is the executor that has that file in its
 * cache, expressed as a TaskLocation.ExecutorCacheTaskLocation, which is the same hint Spark uses
 * for a cached block: the scheduler sends the task there when it can, and sends it anywhere when it
 * cannot. A hint is therefore never a requirement -- a task that lands elsewhere reads its file and
 * caches it there, which is what would happen without the table.
 */
class IndexFileRDD(
    sc: SparkContext,
    locations: Seq[String],
    cacheKeys: Seq[String],
    @transient private val table: IndexCacheLocations,
    locality: Boolean)
  extends RDD[String](sc, Nil) {

  require(
    locations.size == cacheKeys.size,
    s"Each index file needs one cache key: ${locations.size} files, ${cacheKeys.size} keys")

  override protected def getPartitions: Array[Partition] =
    locations.indices.map(new IndexFileRdds.FilePartition(_): Partition).toArray

  override def compute(split: Partition, context: TaskContext): Iterator[String] =
    Iterator.single(locations(split.index))

  /**
   * The hosts that already hold this file.
   *
   * A hint names a host rather than an executor because the scheduler's executor-level location is
   * not public API. On a cluster that runs one executor per node -- which is what the Kubernetes
   * scheduler gives -- a host names the executor as well; where several executors share a host the
   * hint can pick any of them, which still beats sending the task to a host that holds nothing.
   */
  override def getPreferredLocations(split: Partition): Seq[String] =
    IndexFileRdds.preferred(cacheKeys(split.index), table, locality)

  /** The index file a partition reads, so results can be mapped back to it. */
  def locationAt(index: Int): String = locations(index)

  /** A Java-typed view, so the caller does not have to name a ClassTag. */
  def javaRdd: JavaRDD[String] = new JavaRDD[String](this)(ClassTag(classOf[String]))
}

/**
 * The index file a task searches, and the data files it has entries for.
 *
 * A partition carries what its own task reads and nothing more. Handing every task the table's whole
 * scan task list would put the table's file list in each of them, which is the cost the fuse is
 * meant to remove: what a task needs is its index file and the files that file covers.
 */
class IndexFileTask(
    val location: String,
    val entryCount: Long,
    val tasks: java.util.List[FileScanTask])
  extends Serializable

/**
 * One [[IndexFileTask]] per partition, each preferring the executors that already hold its file.
 *
 * The locality hint works the same way as in [[IndexFileRDD]]: it is a hint, so a task that lands
 * elsewhere reads its file and caches it there.
 */
class IndexFileTaskRDD(
    sc: SparkContext,
    files: Seq[IndexFileTask],
    cacheKeys: Seq[String],
    @transient private val table: IndexCacheLocations,
    locality: Boolean)
  extends RDD[IndexFileTask](sc, Nil) {

  require(
    files.size == cacheKeys.size,
    s"Each index file needs one cache key: ${files.size} files, ${cacheKeys.size} keys")

  override protected def getPartitions: Array[Partition] =
    files.indices.map(new IndexFileRdds.FilePartition(_): Partition).toArray

  override def compute(split: Partition, context: TaskContext): Iterator[IndexFileTask] =
    Iterator.single(files(split.index))

  override def getPreferredLocations(split: Partition): Seq[String] =
    IndexFileRdds.preferred(cacheKeys(split.index), table, locality)
}

object IndexFileRdds {

  /**
   * A Java-typed view of the RDD.
   *
   * Named rather than built at the call site so the Java side never has to name a ClassTag or a
   * Scala Seq; the companion's method is reached through its module class.
   */
  def javaRdd(
      sc: SparkContext,
      locations: java.util.List[String],
      cacheKeys: java.util.List[String],
      table: IndexCacheLocations,
      locality: Boolean): JavaRDD[String] =
    new IndexFileRDD(sc, locations.asScala.toSeq, cacheKeys.asScala.toSeq, table, locality).javaRdd

  /**
   * A Java-typed view of the RDD that carries each index file with the files it covers.
   */
  def tasks(
      sc: SparkContext,
      files: java.util.List[IndexFileTask],
      cacheKeys: java.util.List[String],
      table: IndexCacheLocations,
      locality: Boolean): JavaRDD[IndexFileTask] =
    new JavaRDD[IndexFileTask](
      new IndexFileTaskRDD(sc, files.asScala.toSeq, cacheKeys.asScala.toSeq, table, locality))(
      ClassTag(classOf[IndexFileTask]))

  /**
   * The hosts that already hold an index file.
   *
   * A hint names a host rather than an executor because the scheduler's executor-level location is
   * not public API. On a cluster that runs one executor per node -- which is what the Kubernetes
   * scheduler gives -- a host names the executor as well; where several executors share a host the
   * hint can pick any of them, which still beats sending the task to a host that holds nothing.
   */
  private[spark] def preferred(
      cacheKey: String,
      table: IndexCacheLocations,
      locality: Boolean): Seq[String] = {
    if (!locality || table == null) {
      Seq.empty
    } else {
      table
        .holders(cacheKey, 2)
        .asScala
        .flatMap(holder =>
          if (holder.host() == null || holder.host() == "unknown") None else Some(holder.host()))
        .toSeq
    }
  }

  class FilePartition(val index: Int) extends Partition
}
