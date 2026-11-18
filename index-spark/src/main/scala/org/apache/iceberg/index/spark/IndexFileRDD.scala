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
 * cannot. A task that lands elsewhere reads its file from storage.
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

  @transient private lazy val confirmed =
    IndexFileRdds.confirmedLocations(sparkContext, cacheKeys, locality)

  override protected def getPartitions: Array[Partition] =
    locations.indices.map(new IndexFileRdds.FilePartition(_): Partition).toArray

  override def compute(split: Partition, context: TaskContext): Iterator[String] =
    Iterator.single(locations(split.index))

  override def getPreferredLocations(split: Partition): Seq[String] =
    IndexFileRdds.preferred(cacheKeys(split.index), table, locality, confirmed)

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
 *
 * The length is what the index manifest recorded for the file, or zero when it recorded none. It
 * lets the task open the file without asking storage how long it is.
 */
class IndexFileTask(
    val location: String,
    val entryCount: Long,
    val fileLength: Long,
    val tasks: java.util.List[FileScanTask])
  extends Serializable

/**
 * One [[IndexFileTask]] per partition, each preferring the executors that already hold its file.
 *
 * The locality hint works the same way as in [[IndexFileRDD]].
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

  @transient private lazy val confirmed =
    IndexFileRdds.confirmedLocations(sparkContext, cacheKeys, locality)

  override protected def getPartitions: Array[Partition] =
    files.indices.map(new IndexFileRdds.FilePartition(_): Partition).toArray

  override def compute(split: Partition, context: TaskContext): Iterator[IndexFileTask] =
    Iterator.single(files(split.index))

  override def getPreferredLocations(split: Partition): Seq[String] =
    IndexFileRdds.preferred(cacheKeys(split.index), table, locality, confirmed)
}

object IndexFileRdds {

  /** One refresh probe per executor, preferring that executor's process. */
  def statusProbes(sc: SparkContext, locations: java.util.Collection[String]): JavaRDD[String] = {
    val probes =
      if (locations.isEmpty) sc.parallelize(Seq(""), 1)
      else sc.makeRDD(locations.asScala.map(location => (location, Seq(location))).toSeq)
    new JavaRDD[String](probes)(ClassTag(classOf[String]))
  }

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

  private[spark] def confirmedLocations(
      sc: SparkContext,
      cacheKeys: Seq[String],
      locality: Boolean): java.util.Map[String, java.util.List[String]] =
    if (locality) IndexCacheLocations.confirmedLocations(sc, cacheKeys.asJava)
    else java.util.Collections.emptyMap()

  /** Prefer copies acknowledged by the residency plugin, then hosts reported by earlier tasks. */
  private[spark] def preferred(
      cacheKey: String,
      table: IndexCacheLocations,
      locality: Boolean,
      confirmed: java.util.Map[String, java.util.List[String]]): Seq[String] = {
    if (!locality || table == null) {
      Seq.empty
    } else if (confirmed.containsKey(cacheKey)) {
      confirmed.get(cacheKey).asScala.toSeq
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
