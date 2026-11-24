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
package org.apache.iceberg.spark.index

import java.lang.{Long => JLong}
import java.util.{List => JList}
import org.apache.iceberg.SerializableTable
import org.apache.iceberg.Snapshot
import org.apache.iceberg.Table
import org.apache.iceberg.index.IndexFile
import org.apache.iceberg.index.IndexMetadata
import org.apache.iceberg.index.IndexReader
import org.apache.iceberg.index.IndexSnapshot
import org.apache.iceberg.index.spark.IndexEntries
import org.apache.iceberg.index.spark.VectorIndexes
import org.apache.iceberg.index.vector.VectorIndexAdapter
import org.apache.iceberg.index.vector.VectorIndexDefinition
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import scala.collection.mutable
private[spark] object BindVectorSearch {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def bind(spark: SparkSession, request: VectorSearchRequest): VectorBindingResult = {
    val bindStart = System.nanoTime()
    val input = request.input
    val liveTable = if (input.table == null) {
      Spark3Util.loadIcebergTable(spark, input.tableName)
    } else {
      input.table
    }
    // the caller already holds the loaded metadata: hand it to the copy instead of letting it read
    // the metadata file again on every bind (that read is one object-store GET per query)
    val frozen = SerializableTable.copyOfLoaded(liveTable)
    val snapshot = if (input.tableSnapshotId == null) {
      frozen.currentSnapshot()
    } else {
      frozen.snapshot(input.tableSnapshotId.longValue())
    }
    if (snapshot == null) {
      require(
        input.tableSnapshotId == null,
        s"Cannot read vector table snapshot ${input.tableSnapshotId}")
      return NoApplicableVectorIndex
    }
    // what the table metadata read cost: the snapshot resolution is what forces it
    val afterSnapshotNanos = System.nanoTime()
    val keys = Set(
      VectorQueryParameters.RefineFactorConf,
      VectorQueryParameters.IndexNameConf,
      VectorQueryParameters.FilterModeConf,
      VectorQueryParameters.NprobeConf,
      // the name this setting used to have: kept in the view so the capture can refuse it by name
      // instead of Spark ignoring a key nobody reads
      VectorQueryParameters.RenamedOverSampleConf)
    val session = spark.conf.getAll.filter { case (key, _) => keys.contains(key) }.asJava
    val submit = spark.sparkContext.getConf.getAll.toMap.filter { case (key, _) =>
      keys.contains(key)
    }.asJava
    val raw =
      VectorQueryParameters.capture(
        session,
        submit,
        input.indexArgument,
        input.nprobeArgument,
        input.refineFactorArgument)
    val metadata = if (raw.indexName != null) {
      require(
        raw.indexName.value != null && raw.indexName.value.trim.nonEmpty,
        s"Invalid ${VectorQueryParameters.IndexNameConf}: '${raw.indexName.value}'")
      VectorIndexes.findIndex(frozen, input.column, "vector", raw.indexName.value.trim)
    } else {
      val indexes = VectorIndexes.indexesFor(frozen, input.column, "vector")
      if (indexes.size() > 1) {
        return AmbiguousVectorIndex
      }
      if (indexes.isEmpty) {
        return NoApplicableVectorIndex
      }
      indexes.get(0)
    }
    val afterIndexLookupNanos = System.nanoTime()
    // the two index views answer with what the index itself holds, so they read its latest snapshot:
    // a hit of a row that was deleted after the build is still one of the index's hits, and a caller
    // who reindexed the table does not have to read an older index to see what it now ranks. A
    // coverage mode answers for a table snapshot, so it reads the index snapshot written for that
    // snapshot or one of its ancestors.
    val indexView =
      input.contract == VectorResultContract.IndexHits ||
        input.contract == VectorResultContract.IndexPositions
    val indexSnapshot =
      if (indexView) {
        metadata.latestSnapshot()
      } else {
        snapshotAtOrBefore(frozen, snapshot, metadata)
      }
    if (indexSnapshot == null) {
      return NoApplicableVectorIndex
    }
    val definition = metadata.definition().asInstanceOf[VectorIndexDefinition]
    // the column is looked up in the table's schema rather than the snapshot's: a column added
    // after a snapshot was written belongs to the table, and the rows of that snapshot simply have
    // no value for it, which is a row without a vector rather than a query that cannot be answered
    val schema = frozen.schema()
    val field = schema.findField(input.column)
    require(field != null, s"Cannot find vector column ${input.column} in table ${frozen.name()}")
    require(
      definition.fieldId() == field.fieldId(),
      s"Vector index ${metadata.indexUuid()} does not match column ${input.column}")
    val scanTasks = IndexEntries.scanTasks(frozen, snapshot.snapshotId()).asScala.toVector.asJava
    val afterDataPlanNanos = System.nanoTime()
    val dataFiles = scanTasks.asScala.map(_.file().location().toString).toSet
    val files = Vector.newBuilder[IndexFile]
    val covered = mutable.Set.empty[String]
    val uri = indexSnapshot.indexManifestList()
    // the manifests are read through the reader's cache: load_index has already parsed them, and a
    // repeated bind over the same index snapshot must not read them from storage again
    val manifests = new IndexReader(metadata, frozen).manifests(indexSnapshot)
    val indexManifests = manifests.size()
    manifests.asScala.foreach { manifest =>
      covered ++= manifest.indexedDataFiles().asScala
      manifest.indexFiles().asScala.foreach { file =>
        val searchable = file.kind() match {
          case IndexFile.Kind.ENTRY_BEARING =>
            file.blobType() == VectorIndexAdapter.ENTRY_BLOB_TYPE
          case IndexFile.Kind.AUXILIARY =>
            file.blobType() == VectorIndexAdapter.AUXILIARY_BLOB_TYPE
        }
        if (searchable) {
          files += file
        }
      }
    }
    val afterManifestReadsNanos = System.nanoTime()
    val descriptors = files.result()
    val inputs = ResolvedExecutionInputs(
      frozen,
      frozen.asInstanceOf[SerializableTable].metadataFileLocation(),
      snapshot,
      indexSnapshot,
      metadata,
      uri,
      descriptors,
      scanTasks,
      dataFiles,
      SparkSchemaUtil.convert(schema),
      dataFiles -- covered,
      entryUpperBound(descriptors.asJava),
      definition)
    if (logger.isInfoEnabled) {
      logger.info(
        "vector search bind: total={}ms tableMeta={}ms indexLookup={}ms dataPlan={}ms " +
          "indexManifests={}ms reads={indexRegistry:1,indexManifestList:1,indexManifests:{},"
          + "dataPlanFiles:1}",
        Array[Object](
          JLong.valueOf((afterManifestReadsNanos - bindStart) / 1000000L),
          JLong.valueOf((afterSnapshotNanos - bindStart) / 1000000L),
          JLong.valueOf((afterIndexLookupNanos - afterSnapshotNanos) / 1000000L),
          JLong.valueOf((afterDataPlanNanos - afterIndexLookupNanos) / 1000000L),
          JLong.valueOf((afterManifestReadsNanos - afterDataPlanNanos) / 1000000L),
          Integer.valueOf(indexManifests)): _*)
    }
    BoundVectorSearch(VectorSearchContext(request, inputs, raw))
  }

  def entryUpperBound(files: JList[IndexFile]): JLong = {
    val unique = mutable.Map.empty[(String, String), Long]
    var total = 0L
    val iterator = files.iterator()
    while (iterator.hasNext) {
      val file = iterator.next()
      // every searchable payload file can carry entries: a flat structure keeps them in the
      // entry-bearing payload, while a structure over it keeps them in the auxiliary one. Counting
      // only one kind would report an empty index for the other, and an upper bound that is too
      // small would cut candidates that exist.
      val identity = (file.location(), file.blobType())
      unique.get(identity) match {
        case Some(count) if count != file.entryCount() => return null
        case Some(_) =>
        case None =>
          try {
            total = Math.addExact(total, file.entryCount())
          } catch {
            case _: ArithmeticException => return null
          }
          unique.put(identity, file.entryCount())
      }
    }
    JLong.valueOf(total)
  }

  private def snapshotAtOrBefore(
      table: Table,
      readSnapshot: Snapshot,
      metadata: IndexMetadata): IndexSnapshot = {
    var ancestor = readSnapshot
    while (ancestor != null) {
      val candidate = metadata.snapshotForSourceTableSnapshot(ancestor.snapshotId())
      if (candidate != null) {
        return candidate
      }
      val parent = ancestor.parentId()
      ancestor = if (parent == null) null else table.snapshot(parent.longValue())
    }
    null
  }
}
