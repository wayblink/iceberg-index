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
package org.apache.iceberg.index;

import com.sun.management.HotSpotDiagnosticMXBean;
import java.lang.management.ManagementFactory;

/** Shared HotSpot layout assumptions for estimating index cache residency. */
public final class IndexMemoryLayout {
  public static final int HEADER_BYTES = 16;
  public static final int REFERENCE_BYTES = 4;
  public static final int ALIGNMENT = 8;
  private static final String UNSUPPORTED = probeLayout();

  private IndexMemoryLayout() {}

  public static boolean supported() {
    return UNSUPPORTED == null;
  }

  public static String unsupportedReason() {
    return UNSUPPORTED;
  }

  /** Returns the memory model's runtime profile, or null for an unsupported JVM. */
  public static String profile(int processors) {
    return supported()
        ? "hotspot-17-64;object-header-allowance="
            + HEADER_BYTES
            + ";array-header="
            + HEADER_BYTES
            + ";reference-bytes="
            + REFERENCE_BYTES
            + ";alignment="
            + ALIGNMENT
            + ";compact-strings=true;representation=in-place;processors="
            + processors
        : null;
  }

  /** Rounds an object or array's header and fields up to the model's alignment. */
  public static long alignedSize(long fields) {
    return (HEADER_BYTES + fields + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT;
  }

  private static String probeLayout() {
    String version = System.getProperty("java.specification.version");
    if (!"17".equals(version)) {
      return "the retained-memory model describes HotSpot 17, this JVM is " + version + ".";
    }

    if (!"64".equals(System.getProperty("sun.arch.data.model"))) {
      return "the retained-memory model describes a 64-bit JVM.";
    }

    try {
      HotSpotDiagnosticMXBean bean =
          ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
      if (bean == null) {
        return "this JVM exposes no HotSpot diagnostic options to confirm its layout.";
      }

      String compressedOops = bean.getVMOption("UseCompressedOops").getValue();
      if (!"true".equals(compressedOops)) {
        return "this JVM runs with UseCompressedOops=" + compressedOops + ".";
      }

      String compressedClassPointers = bean.getVMOption("UseCompressedClassPointers").getValue();
      if (!"true".equals(compressedClassPointers)) {
        return "this JVM runs with UseCompressedClassPointers=" + compressedClassPointers + ".";
      }

      String alignment = bean.getVMOption("ObjectAlignmentInBytes").getValue();
      if (!"8".equals(alignment)) {
        return "this JVM runs with ObjectAlignmentInBytes=" + alignment + ".";
      }

      String compactStrings = bean.getVMOption("CompactStrings").getValue();
      if (!"true".equals(compactStrings)) {
        return "this JVM runs with CompactStrings=" + compactStrings + ".";
      }

      return null;
    } catch (RuntimeException | LinkageError e) {
      return "this JVM does not expose the layout options the model depends on (" + e + ").";
    }
  }
}
