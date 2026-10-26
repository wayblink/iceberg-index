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

import java.io.{IOException, UncheckedIOException}
import java.lang.{Long => JLong}
import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.iceberg.{SerializableTable, Snapshot, Table}
import org.apache.iceberg.index.{IndexFile, IndexManifest, IndexManifestList, IndexMetadata, IndexSnapshot}
import org.apache.iceberg.index.spark.{IndexEntries, VectorIndexes}
import org.apache.iceberg.index.vector.{VectorIndexAdapter, VectorIndexDefinition}
import org.apache.iceberg.spark.{Spark3Util, SparkSchemaUtil}
import org.apache.iceberg.util.SnapshotUtil
import org.apache.spark.sql.SparkSession

private[spark] object BindVectorSearch {
  def bind(spark: SparkSession, request: VectorSearchRequest): VectorBindingResult = {
    val input = request.input
    val liveTable = if (input.table == null) {
      Spark3Util.loadIcebergTable(spark, input.tableName)
    } else {
      input.table
    }
    val frozen = SerializableTable.copyOf(liveTable)
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
    val keys = Set(VectorQueryParameters.OverSampleConf, VectorQueryParameters.IndexNameConf)
    val session = spark.conf.getAll.filter { case (key, _) => keys.contains(key) }.asJava
    val submit = spark.sparkContext.getConf.getAll.toMap.filter { case (key, _) =>
      keys.contains(key)
    }.asJava
    val raw = VectorQueryParameters.capture(session, submit, input.indexArgument)
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
    val indexSnapshot = if (input.contract == VectorResultContract.IndexHits) {
      metadata.latestSnapshot()
    } else {
      snapshotAtOrBefore(frozen, snapshot, metadata)
    }
    if (indexSnapshot == null) {
      return NoApplicableVectorIndex
    }
    val definition = metadata.definition().asInstanceOf[VectorIndexDefinition]
    val schema = SnapshotUtil.schemaFor(frozen, snapshot.snapshotId())
    val field = schema.findField(input.column)
    require(
      field != null,
      s"Cannot find vector column ${input.column} in snapshot ${snapshot.snapshotId()}")
    require(
      definition.fieldId() == field.fieldId(),
      s"Vector index ${metadata.indexUuid()} does not match column ${input.column}")
    val scanTasks = IndexEntries.scanTasks(frozen, snapshot.snapshotId()).asScala.toVector.asJava
    val dataFiles = scanTasks.asScala.map(_.file().location().toString).toSet
    val files = Vector.newBuilder[IndexFile]
    val covered = mutable.Set.empty[String]
    val uri = indexSnapshot.indexManifestList()
    try {
      val manifestList = IndexManifestList.read(frozen.io().newInputFile(uri))
      manifestList.manifests().asScala.foreach { ref =>
        val manifest = IndexManifest.read(frozen.io().newInputFile(ref.location()))
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
    } catch {
      case cause: IOException =>
        throw new UncheckedIOException(s"Cannot read bound vector index manifests at $uri", cause)
    }
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
