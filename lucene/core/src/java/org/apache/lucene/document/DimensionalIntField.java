package org.apache.lucene.document;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;

/** An int field that is indexed dimensionally such that finding
 *  all documents within an N-dimensional shape or range at search time is
 *  efficient.  Muliple values for the same field in one documents
 *  is allowed. */

public final class DimensionalIntField extends Field {

  private static FieldType getType(int numDims) {
    FieldType type = new FieldType();
    type.setDimensions(numDims, RamUsageEstimator.NUM_BYTES_INT);
    type.freeze();
    return type;
  }

  @Override
  public void setIntValue(int value) {
    setIntValues(value);
  }

  /** Change the values of this field */
  public void setIntValues(int... point) {
    fieldsData = pack(point);
  }

  @Override
  public void setBytesValue(BytesRef bytes) {
    throw new IllegalArgumentException("cannot change value type from int to BytesRef");
  }

  @Override
  public Number numericValue() {
    BytesRef bytes = (BytesRef) fieldsData;
    assert bytes.length == RamUsageEstimator.NUM_BYTES_INT;
    return NumericUtils.bytesToInt(bytes.bytes, bytes.offset);
  }

  private static BytesRef pack(int... point) {
    if (point == null) {
      throw new IllegalArgumentException("point cannot be null");
    }
    if (point.length == 0) {
      throw new IllegalArgumentException("point cannot be 0 dimensions");
    }
    byte[] packed = new byte[point.length * RamUsageEstimator.NUM_BYTES_INT];
    
    for(int dim=0;dim<point.length;dim++) {
      NumericUtils.intToBytes(point[dim], packed, dim);
    }

    return new BytesRef(packed);
  }

  /** Creates a new DimensionalIntField, indexing the
   *  provided N-dimensional int point.
   *
   *  @param name field name
   *  @param point int[] value
   *  @throws IllegalArgumentException if the field name or value is null.
   */
  public DimensionalIntField(String name, int... point) {
    super(name, pack(point), getType(point.length));
  }
}
