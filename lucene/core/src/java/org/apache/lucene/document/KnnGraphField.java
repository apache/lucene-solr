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
 * Field that models a graph that supports (approximate) KNN (K-nearest-neighbor) search.  Each
 * document (that is in the graph) is represented by a vector {@code float[]} value with a the
 * specified length.  <p> Here's an example usage: </p>
 *
 * <pre class="prettyprint">
 *   document.add(new KnnGraphField(name, new float[]{1f, 2f, 3f}));
 * </pre>
 *
 */
public class KnnGraphField extends Field {
  
  public static final String SUBTYPE_ATTR = "subtype";
  public static final String KNN_GRAPH = "knn-graph";

  private static final ConcurrentHashMap<Integer, FieldType> TYPES= new ConcurrentHashMap<>();

  private static FieldType createType(int dimension) {
    if (dimension <= 0) {
      throw new IllegalArgumentException("dimension must be positive, not " + dimension);
    }
    FieldType type = new FieldType();
    type.setDocValuesType(DocValuesType.BINARY);
    type.putAttribute(VectorDocValues.DIMENSION_ATTR, Integer.toString(dimension));
    type.putAttribute(SUBTYPE_ATTR, KNN_GRAPH);
    type.freeze();
    return type;
  }

  /**
   * Dimensioned types; one type per vector length
   */
  public static FieldType type(int dimension) {
    return TYPES.computeIfAbsent(dimension, KnnGraphField::createType);
  }

  /**
   * Create a new binary KnnGraph field wrapping an array of floats.
   * @param name field name
   * @param value vector of float values as an array
   * @throws IllegalArgumentException if the field name is null
   */
  public KnnGraphField(String name, float[] value) {
    super(name, type(value.length));
    fieldsData = value;
  }

  public float[] vectorValue() {
    return (float[]) fieldsData;
  }
}
