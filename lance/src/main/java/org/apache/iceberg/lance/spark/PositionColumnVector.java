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
package org.apache.iceberg.lance.spark;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A column vector for Iceberg's {@code _pos} metadata column.
 *
 * <p>{@code _pos} is the row's ordinal in the data file, so it varies per row and cannot be a
 * constant column. The vector holds the file offset of the first row in the batch and adds the row
 * index on access.
 */
class PositionColumnVector extends ColumnVector {

  private final long startRow;

  PositionColumnVector(long startRow) {
    super(DataTypes.LongType);
    this.startRow = startRow;
  }

  @Override
  public void close() {}

  @Override
  public boolean hasNull() {
    return false;
  }

  @Override
  public int numNulls() {
    return 0;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return false;
  }

  @Override
  public boolean getBoolean(int rowId) {
    throw new UnsupportedOperationException("_pos is a long column");
  }

  @Override
  public byte getByte(int rowId) {
    throw new UnsupportedOperationException("_pos is a long column");
  }

  @Override
  public short getShort(int rowId) {
    throw new UnsupportedOperationException("_pos is a long column");
  }

  @Override
  public int getInt(int rowId) {
    return (int) getLong(rowId);
  }

  @Override
  public long getLong(int rowId) {
    return startRow + rowId;
  }

  @Override
  public float getFloat(int rowId) {
    throw new UnsupportedOperationException("_pos is a long column");
  }

  @Override
  public double getDouble(int rowId) {
    throw new UnsupportedOperationException("_pos is a long column");
  }

  @Override
  public org.apache.spark.sql.types.Decimal getDecimal(int rowId, int precision, int scale) {
    throw new UnsupportedOperationException("_pos is a long column");
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    throw new UnsupportedOperationException("_pos is a long column");
  }

  @Override
  public byte[] getBinary(int rowId) {
    throw new UnsupportedOperationException("_pos is a long column");
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new UnsupportedOperationException("_pos is a long column");
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    throw new UnsupportedOperationException("_pos is a long column");
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    throw new UnsupportedOperationException("_pos has no children");
  }
}
