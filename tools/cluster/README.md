<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Cluster assets: the index build entry point and the image

Everything here belongs to the product, not to one experiment: these are the things a cluster needs
in order to build an index and to run the code that queries it. They used to live in a scratch
directory on the shared dev box (`/data/workspace/sparkann/`), which is not a repository and is
written by several people at once, so nothing recorded which commit an image or a build tool came
from. That is exactly how the image ended up unable to read the manifests its own build tool wrote
(#2756): two artifacts, two provenance, no check.

## What is here

| File | What it is | Where it is used |
|---|---|---|
| `ClusterIndexBuild.java` | Cluster-side entry point for a build: `spark-submit` has no shell, so it makes the same `CALL build_vector_index(...)` a session would | driver of a build job |
| `cluster-build-submit.sh` | Submits that job; `STRUCT` / `QUANT` / `OPTS` / `NAME` in the environment pick the index parameters | build job |
| `Dockerfile` | The Spark image: removes the vendor `iceberg-spark-runtime`, `parquet-*` and `avro-1.11` jars and copies ours in | image build |
| `image-jars.txt` | Names and sha256 of the jars that go into `/opt/spark/jars` | image build, provenance |

The jars themselves are not committed. `image-jars.txt` lists them so that an image can be checked
after the fact: rebuild the jars from a commit, hash them, and compare.

## What is deliberately not here

The scratch directory also holds one-off experiment scripts (`build-*.sh`, `armcurve*`, `probe*`),
the SIFT data loaders, and other people's working directories. They are not product assets, they
change per experiment, and putting them in the repository would suggest they are supported.

## Building an image

```bash
# 1. jars: the iceberg jars come from a commit, the rest are pinned third-party artifacts
./gradlew :iceberg-core:assemble :iceberg-index-vector:assemble :iceberg-index-text:assemble \
          :iceberg-index-spark:assemble :iceberg-lance:assemble :iceberg-parquet:assemble \
          :iceberg-orc:assemble :iceberg-arrow:assemble :iceberg-data:assemble \
          :iceberg-hive-metastore:assemble :iceberg-bundled-guava:assemble \
          :iceberg-spark:iceberg-spark-3.5_2.12:assemble \
          :iceberg-spark:iceberg-spark-extensions-3.5_2.12:assemble -DsparkVersions=3.5

# 2. lay them out next to this file as image-jars/ and check them against the list
(cd image-jars && sha256sum -c ../tools/cluster/image-jars.txt)

# 3. build and push, tagging with the commit so an image can be traced back
docker build -t <registry>/sparkann-e2e:$(date +%Y%m%d)-$(git rev-parse --short HEAD) .
```

## The check that was missing

A build tool and an image are built from the same commit, but nothing enforced it, and a manifest
format that the writer bumped (v2) while the reader only accepted v1 turned every `list_indexes`
call into a failure. Before publishing an image, verify the reader's accepted versions include the
writer's:

```bash
# the writer's version, from the source
grep -n "FORMAT_VERSION = " core/src/main/java/org/apache/iceberg/index/IndexManifest.java

# the reader's accepted set, from the jar that goes into the image
unzip -p image-jars/iceberg-core-*.jar org/apache/iceberg/index/IndexManifest.class \
  | strings | grep -i "Unsupported index manifest format version"
```

and run one real call against a table that has indexes:

```sql
CALL catalog.system.list_indexes(table => 'db.t');
```

It must return every index on the table. A failure here is a release blocker, not a bug to file
later: the build tool and the runtime disagree about a format on disk.

## Known gap

`ClusterIndexBuild` is compiled by hand into `cluster-build.jar` on the dev box, so its jar is still
an artifact with no build recipe. Until that is a gradle task, treat the source here as the truth
and rebuild it whenever the repository moves.
