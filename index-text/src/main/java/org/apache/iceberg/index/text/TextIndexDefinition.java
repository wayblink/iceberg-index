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
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

/** Definition of a BM25 text index over a string column. */
public final class TextIndexDefinition implements IndexDefinition {

  private static final long serialVersionUID = 1L;

  private final int fieldId;
  private final double k1;
  private final double lengthNorm;
  private final int minTokenLength;
  private final boolean lowercase;
  private final Set<String> stopwords;
  private final Map<String, String> properties;

  private TextIndexDefinition(
      int fieldId,
      double k1,
      double lengthNorm,
      int minTokenLength,
      boolean lowercase,
      Set<String> stopwords,
      Map<String, String> properties) {
    this.fieldId = fieldId;
    this.k1 = k1;
    this.lengthNorm = lengthNorm;
    this.minTokenLength = minTokenLength;
    this.lowercase = lowercase;
    this.stopwords = ImmutableSet.copyOf(stopwords);
    this.properties = ImmutableMap.copyOf(properties);
  }

  @Override
  public String type() {
    return TextIndexType.TYPE;
  }

  /** The field ID of the indexed string column. */
  public int fieldId() {
    return fieldId;
  }

  /** The BM25 term frequency saturation parameter. */
  public double k1() {
    return k1;
  }

  /** The BM25 document length normalization parameter. */
  public double lengthNorm() {
    return lengthNorm;
  }

  /** Tokens shorter than this are dropped when analyzing documents and queries. */
  public int minTokenLength() {
    return minTokenLength;
  }

  /** Whether documents and queries are lowercased before tokenization. */
  public boolean lowercase() {
    return lowercase;
  }

  /** Tokens that are dropped when analyzing documents and queries. */
  public Set<String> stopwords() {
    return stopwords;
  }

  /** Additional user-defined properties of this definition. */
  public Map<String, String> properties() {
    return properties;
  }

  /** Analyzes text into the tokens this index stores. */
  public List<String> analyze(String text) {
    return TextAnalyzer.analyze(text, lowercase, minTokenLength, stopwords);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof TextIndexDefinition)) {
      return false;
    }
    TextIndexDefinition that = (TextIndexDefinition) other;
    return fieldId == that.fieldId
        && Double.compare(k1, that.k1) == 0
        && Double.compare(lengthNorm, that.lengthNorm) == 0
        && minTokenLength == that.minTokenLength
        && lowercase == that.lowercase
        && stopwords.equals(that.stopwords)
        && properties.equals(that.properties);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(
        fieldId, k1, lengthNorm, minTokenLength, lowercase, stopwords, properties);
  }

  @Override
  public String toString() {
    return "TextIndexDefinition{"
        + "fieldId="
        + fieldId
        + ", k1="
        + k1
        + ", lengthNorm="
        + lengthNorm
        + ", minTokenLength="
        + minTokenLength
        + ", lowercase="
        + lowercase
        + ", stopwords="
        + stopwords
        + '}';
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private Integer fieldId;
    private double k1 = 1.2;
    private double lengthNorm = 0.75;
    private int minTokenLength = 2;
    private boolean lowercase = true;
    private final ImmutableSet.Builder<String> stopwords = ImmutableSet.builder();
    private final ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

    private Builder() {}

    public Builder fieldId(int newFieldId) {
      this.fieldId = newFieldId;
      return this;
    }

    public Builder k1(double newK1) {
      this.k1 = newK1;
      return this;
    }

    public Builder lengthNorm(double newLengthNorm) {
      this.lengthNorm = newLengthNorm;
      return this;
    }

    public Builder minTokenLength(int newMinTokenLength) {
      this.minTokenLength = newMinTokenLength;
      return this;
    }

    public Builder lowercase(boolean newLowercase) {
      this.lowercase = newLowercase;
      return this;
    }

    public Builder stopword(String stopword) {
      this.stopwords.add(stopword);
      return this;
    }

    public Builder stopwords(Iterable<String> newStopwords) {
      this.stopwords.addAll(newStopwords);
      return this;
    }

    public Builder property(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    public Builder properties(Map<String, String> newProperties) {
      this.properties.putAll(newProperties);
      return this;
    }

    public TextIndexDefinition build() {
      return new TextIndexDefinition(
          fieldId,
          k1,
          lengthNorm,
          minTokenLength,
          lowercase,
          stopwords.build(),
          properties.build());
    }
  }
}
