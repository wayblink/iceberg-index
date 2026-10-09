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
package org.apache.iceberg.index.text;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * The analyzer of text indexes: splits text into tokens on non-alphanumeric boundaries, optionally
 * lowercases them and drops short tokens and stopwords.
 */
final class TextAnalyzer {

  private TextAnalyzer() {}

  static List<String> analyze(
      String text, boolean lowercase, int minTokenLength, java.util.Set<String> stopwords) {
    List<String> tokens = Lists.newArrayList();
    if (text == null) {
      return tokens;
    }

    String normalized = lowercase ? text.toLowerCase(java.util.Locale.ROOT) : text;
    int start = -1;
    for (int i = 0; i <= normalized.length(); i++) {
      boolean isTokenChar = i < normalized.length() && Character.isLetterOrDigit(normalized.charAt(i));
      if (isTokenChar) {
        if (start < 0) {
          start = i;
        }
      } else if (start >= 0) {
        addToken(tokens, normalized.substring(start, i), minTokenLength, stopwords);
        start = -1;
      }
    }
    return tokens;
  }

  private static void addToken(
      List<String> tokens, String token, int minTokenLength, java.util.Set<String> stopwords) {
    if (token.length() >= minTokenLength && !stopwords.contains(token)) {
      tokens.add(token);
    }
  }
}
