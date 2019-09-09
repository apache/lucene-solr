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

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.VectorDocValues;
import org.apache.lucene.util.BytesRef;

/**
 * Field that models a per-document {@code float[]} value with a constant length for all documents having a value.
 * <p>
 * Here's an example usage:
 * </p>
 * 
 * <pre class="prettyprint">
 *   document.add(new VectorDocValuesField(name, new float[]{1f, 2f, 3f}));
 * </pre>
 * 
 * @see org.apache.lucene.index.VectorDocValues
 * */
public class VectorDocValuesField extends Field {
  
  private static final ConcurrentHashMap<Integer, FieldType> TYPES= new ConcurrentHashMap<>();

  private static FieldType createType(int dimension) {
    if (dimension <= 0) {
      throw new IllegalArgumentException("dimension must be positive, not " + dimension);
    }
    FieldType type = new FieldType();
    type.setDocValuesType(DocValuesType.BINARY);
    type.putAttribute(VectorDocValues.DIMENSION_ATTR, Integer.toString(dimension));
    type.freeze();
    return type;
  }

  /**
   * Dimensioned types; one type per vector length
   */
  public static FieldType type(int dimension) {
    return TYPES.computeIfAbsent(dimension, VectorDocValuesField::createType);
  }

  /**
   * Create a new binary DocValues field.
   * @param name field name
   * @param value vector of float values as an array
   * @throws IllegalArgumentException if the field name is null
   */
  public VectorDocValuesField(String name, float[] value) {
    super(name, type(value.length));
    BytesRef bytesRef = new BytesRef(value.length * 4);
    ByteBuffer buf = ByteBuffer.wrap(bytesRef.bytes, 0, value.length * 4);
    buf.asFloatBuffer().put(value);
    bytesRef.length = bytesRef.bytes.length;
    fieldsData = bytesRef;
  }
}
