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
package org.apache.iceberg.index.vector;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.index.vector.NativeIvfBuild.LibraryLoader;
import org.apache.iceberg.index.vector.NativeIvfBuild.LoadState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** What the bridge does when the optional native library cannot be loaded or extracted. */
class TestNativeIvfBuildLoad {

  @Test
  void aFailedLoadIsNotTriedAgain() {
    LoadState state = new LoadState();
    AtomicInteger attempts = new AtomicInteger();
    LibraryLoader libraryPath =
        () -> {
          attempts.incrementAndGet();
          return "no library on the path";
        };
    LibraryLoader resource =
        () -> {
          attempts.incrementAndGet();
          return "no packaged library";
        };

    assertThat(NativeIvfBuild.ensureLoaded(state, libraryPath, resource)).isFalse();
    assertThat(attempts).as("both places are tried once").hasValue(2);
    assertThat(state.failed()).as("the failure is remembered").isTrue();

    assertThat(NativeIvfBuild.ensureLoaded(state, libraryPath, resource))
        .as("a build after a failed load keeps falling back to the Java kernel")
        .isFalse();
    assertThat(attempts).as("a failed load is not extracted or loaded again").hasValue(2);
    assertThat(state.loaded()).isFalse();
  }

  @Test
  void aSuccessfulLoadIsNotTriedAgain() {
    LoadState state = new LoadState();
    AtomicInteger attempts = new AtomicInteger();
    LibraryLoader libraryPath =
        () -> {
          attempts.incrementAndGet();
          return null;
        };
    LibraryLoader resource =
        () -> {
          attempts.incrementAndGet();
          return "the library path already answered";
        };

    assertThat(NativeIvfBuild.ensureLoaded(state, libraryPath, resource)).isTrue();
    assertThat(NativeIvfBuild.ensureLoaded(state, libraryPath, resource)).isTrue();
    assertThat(attempts).as("a loaded library is not loaded again").hasValue(1);
    assertThat(state.loaded()).isTrue();
  }

  @Test
  void aFailedExtractionLeavesNoTemporaryCopy(@TempDir Path directory) throws Exception {
    byte[] notALibrary = "not a native library".getBytes(StandardCharsets.US_ASCII);
    try (InputStream in = new ByteArrayInputStream(notALibrary)) {
      String failure = NativeIvfBuild.extractAndLoad(in, "a test library", directory);
      assertThat(failure).as("a file that is not a library reports the failure").isNotNull();
    }

    try (var files = Files.list(directory)) {
      assertThat(files.toList())
          .as("the extracted copy is deleted when it cannot be loaded")
          .isEmpty();
    }
  }
}
