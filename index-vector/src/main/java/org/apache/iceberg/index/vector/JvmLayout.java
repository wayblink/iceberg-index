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

import com.sun.management.HotSpotDiagnosticMXBean;
import java.lang.management.ManagementFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The one place that decides whether this JVM's object layout is the one the retained-memory model
 * describes.
 *
 * <p>{@link PreparedStructureSize} states that layout as constants: a 16-byte header allowance, a
 * four-byte reference, eight-byte alignment, compact strings. Those constants are wrong on a JVM
 * that does not use compressed references or that aligns differently, and an estimate built on them
 * would be a guess presented as a measurement. So callers gate on this probe and report unknown
 * instead.
 *
 * <p>The answer cannot change while a JVM runs, so it is decided once. A probe per call would read
 * the same MXBean on every estimate, which is on the query path.
 */
public final class JvmLayout {

  private static final Logger LOG = LoggerFactory.getLogger(JvmLayout.class);

  private static final String UNSUPPORTED = probeLayout();

  private JvmLayout() {}

  /** Whether the retained-memory model describes this JVM's object layout. */
  public static boolean supported() {
    return UNSUPPORTED == null;
  }

  /**
   * Why the model does not describe this JVM, or null when it does.
   *
   * <p>For a caller that reports the reason rather than only the consequence.
   */
  public static String unsupportedReason() {
    return UNSUPPORTED;
  }

  /**
   * Whether the model describes this JVM, naming the reason once when it does not.
   *
   * <p>An unsupported layout turns the index cache's capacity model off, which is worth one line
   * wherever it is noticed rather than a debug message per estimate.
   */
  public static boolean supportedOrWarn(String consumer) {
    if (UNSUPPORTED == null) {
      return true;
    }

    LOG.warn(
        "{} is unavailable: {} Retained-memory estimates report unknown and the index cache keeps"
            + " no derived budget.",
        consumer,
        UNSUPPORTED);
    return false;
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
