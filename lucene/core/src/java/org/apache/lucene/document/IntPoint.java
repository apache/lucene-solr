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
package org.apache.lucene.document;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/** An int field that is indexed dimensionally such that finding
 *  all documents within an N-dimensional shape or range at search time is
 *  efficient.  Multiple values for the same field in one documents
 *  is allowed. */

public final class IntPoint extends Field {

  private static FieldType getType(int numDims) {
    FieldType type = new FieldType();
    type.setDimensions(numDims, Integer.BYTES);
    type.freeze();
    return type;
  }

  @Override
  public void setIntValue(int value) {
    setIntValues(value);
  }

  /** Change the values of this field */
  public void setIntValues(int... point) {
    if (type.pointDimensionCount() != point.length) {
      throw new IllegalArgumentException("this field (name=" + name + ") uses " + type.pointDimensionCount() + " dimensions; cannot change to (incoming) " + point.length + " dimensions");
    }
    fieldsData = pack(point);
  }

  @Override
  public void setBytesValue(BytesRef bytes) {
    throw new IllegalArgumentException("cannot change value type from int to BytesRef");
  }

  @Override
  public Number numericValue() {
    if (type.pointDimensionCount() != 1) {
      throw new IllegalStateException("this field (name=" + name + ") uses " + type.pointDimensionCount() + " dimensions; cannot convert to a single numeric value");
    }
    BytesRef bytes = (BytesRef) fieldsData;
    assert bytes.length == Integer.BYTES;
    return decodeDimension(bytes.bytes, bytes.offset);
  }

  private static BytesRef pack(int... point) {
    if (point == null) {
      throw new IllegalArgumentException("point cannot be null");
    }
    if (point.length == 0) {
      throw new IllegalArgumentException("point cannot be 0 dimensions");
    }
    byte[] packed = new byte[point.length * Integer.BYTES];
    
    for (int dim = 0; dim < point.length; dim++) {
      encodeDimension(point[dim], packed, dim * Integer.BYTES);
    }

    return new BytesRef(packed);
  }

  /** Creates a new IntPoint, indexing the
   *  provided N-dimensional int point.
   *
   *  @param name field name
   *  @param point int[] value
   *  @throws IllegalArgumentException if the field name or value is null.
   */
  public IntPoint(String name, int... point) {
    super(name, pack(point), getType(point.length));
  }

  // public helper methods (e.g. for queries)

  /** Encode n-dimensional integer values into binary encoding */
  public static byte[][] encode(Integer value[]) {
    byte[][] encoded = new byte[value.length][];
    for (int i = 0; i < value.length; i++) {
      if (value[i] != null) {
        encoded[i] = new byte[Integer.BYTES];
        encodeDimension(value[i], encoded[i], 0);
      }
    }
    return encoded;
  }
  
  /** Encode single integer dimension */
  public static void encodeDimension(Integer value, byte dest[], int offset) {
    NumericUtils.intToBytes(value, dest, offset);
  }
  
  /** Decode single integer dimension */
  public static Integer decodeDimension(byte value[], int offset) {
    return NumericUtils.bytesToInt(value, offset);
  }
}
