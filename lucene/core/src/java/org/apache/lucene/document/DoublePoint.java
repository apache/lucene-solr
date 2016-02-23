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

/** A double field that is indexed dimensionally such that finding
 *  all documents within an N-dimensional shape or range at search time is
 *  efficient.  Multiple values for the same field in one documents
 *  is allowed. */

public final class DoublePoint extends Field {

  private static FieldType getType(int numDims) {
    FieldType type = new FieldType();
    type.setDimensions(numDims, Double.BYTES);
    type.freeze();
    return type;
  }

  @Override
  public void setDoubleValue(double value) {
    setDoubleValues(value);
  }

  /** Change the values of this field */
  public void setDoubleValues(double... point) {
    if (type.pointDimensionCount() != point.length) {
      throw new IllegalArgumentException("this field (name=" + name + ") uses " + type.pointDimensionCount() + " dimensions; cannot change to (incoming) " + point.length + " dimensions");
    }
    fieldsData = pack(point);
  }

  @Override
  public void setBytesValue(BytesRef bytes) {
    throw new IllegalArgumentException("cannot change value type from double to BytesRef");
  }

  @Override
  public Number numericValue() {
    if (type.pointDimensionCount() != 1) {
      throw new IllegalStateException("this field (name=" + name + ") uses " + type.pointDimensionCount() + " dimensions; cannot convert to a single numeric value");
    }
    BytesRef bytes = (BytesRef) fieldsData;
    assert bytes.length == Double.BYTES;
    return decodeDimension(bytes.bytes, bytes.offset);
  }

  private static BytesRef pack(double... point) {
    if (point == null) {
      throw new IllegalArgumentException("point cannot be null");
    }
    if (point.length == 0) {
      throw new IllegalArgumentException("point cannot be 0 dimensions");
    }
    byte[] packed = new byte[point.length * Double.BYTES];
    
    for (int dim = 0; dim < point.length ; dim++) {
      encodeDimension(point[dim], packed, dim * Double.BYTES);
    }

    return new BytesRef(packed);
  }

  /** Creates a new DoublePoint, indexing the
   *  provided N-dimensional int point.
   *
   *  @param name field name
   *  @param point double[] value
   *  @throws IllegalArgumentException if the field name or value is null.
   */
  public DoublePoint(String name, double... point) {
    super(name, pack(point), getType(point.length));
  }
  
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(type.toString());
    result.append('<');
    result.append(name);
    result.append(':');

    BytesRef bytes = (BytesRef) fieldsData;
    for (int dim = 0; dim < type.pointDimensionCount(); dim++) {
      if (dim > 0) {
        result.append(',');
      }
      result.append(decodeDimension(bytes.bytes, bytes.offset + dim * Double.BYTES));
    }

    result.append('>');
    return result.toString();
  }
  
  // public helper methods (e.g. for queries)

  /** Encode n-dimensional double point into binary encoding */
  public static byte[][] encode(Double value[]) {
    byte[][] encoded = new byte[value.length][];
    for (int i = 0; i < value.length; i++) {
      if (value[i] != null) {
        encoded[i] = new byte[Double.BYTES];
        encodeDimension(value[i], encoded[i], 0);
      }
    }
    return encoded;
  }
  
  /** Encode single double dimension */
  public static void encodeDimension(Double value, byte dest[], int offset) {
    NumericUtils.longToBytesDirect(NumericUtils.doubleToSortableLong(value), dest, offset);
  }
  
  /** Decode single double dimension */
  public static Double decodeDimension(byte value[], int offset) {
    return NumericUtils.sortableLongToDouble(NumericUtils.bytesToLongDirect(value, offset));
  }
}
