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

import org.apache.lucene.index.VectorValues;

/** A field that contains a single floating-point numeric vector (or none) for each document.
 * Vectors are dense - that is, every dimension of a vector contains an explicit value, stored
 * packed into an array (of type float[]) whose length is the vector dimension. Values can be
 * retrieved using {@link VectorValues}, which is a forward-only docID-based iterator and also
 * offers random-access by dense ordinal (not docId). VectorValues.SearchStrategys may be
 * used to compare vectors at query time (for example as part of result ranking). A VectorField may
 * be associated with a search strategy that defines the metric used for nearest-neighbor search
 * among vectors of that field, but at the moment this association is purely nominal: it is intended
 * for future use by the to-be-implemented nearest neighbors search.
 */
public class VectorField extends Field {

  private static FieldType getType(float[] v, VectorValues.SearchStrategy searchStrategy) {
    if (v == null) {
      throw new IllegalArgumentException("vector value must not be null");
    }
    int dimension = v.length;
    if (dimension == 0) {
      throw new IllegalArgumentException("cannot index an empty vector");
    }
    if (dimension > VectorValues.MAX_DIMENSIONS) {
      throw new IllegalArgumentException("cannot index vectors with dimension greater than " + VectorValues.MAX_DIMENSIONS);
    }
    if (searchStrategy == null) {
      throw new IllegalArgumentException("search strategy must not be null");
    }
    FieldType type = new FieldType();
    type.setVectorDimensionsAndSearchStrategy(dimension, searchStrategy);
    type.freeze();
    return type;
  }

  /** Creates a numeric vector field. Fields are single-valued: each document has either one value
   * or no value. Vectors of a single field share the same dimension and search strategy. Note that some strategies
   * (notably dot-product) require values to be unit-length, which can be enforced using VectorUtil.l2Normalize(float[]).
   *
   *  @param name field name
   *  @param vector value
   *  @param searchStrategy a function defining vector proximity.
   *  @throws IllegalArgumentException if any parameter is null, or the vector is empty or has dimension &gt; 1024.
   */
  public VectorField(String name, float[] vector, VectorValues.SearchStrategy searchStrategy) {
    super(name, getType(vector, searchStrategy));
    fieldsData = vector;
  }

  /** Creates a numeric vector field with the default EUCLIDEAN_HNSW (L2) search strategy. Fields are
   * single-valued: each document has either one value or no value. Vectors of a single field share
   * the same dimension and search strategy.
   *
   *  @param name field name
   *  @param vector value
   *  @throws IllegalArgumentException if any parameter is null, or the vector is empty or has dimension &gt; 1024.
   */
  public VectorField(String name, float[] vector) {
    this(name, vector, VectorValues.SearchStrategy.EUCLIDEAN_HNSW);
  }

  /**
   * Return the vector value of this field
   */
  public float[] vectorValue() {
    return (float[]) fieldsData;
  }

  /**
   * Set the vector value of this field
   * @param value the value to set; must not be null, and length must match the field type
   */
  public void setVectorValue(float[] value) {
    if (value == null) {
      throw new IllegalArgumentException("value must not be null");
    }
    if (value.length != type.vectorDimension()) {
      throw new IllegalArgumentException("value length " + value.length + " must match field dimension " + type.vectorDimension());
    }
    fieldsData = value;
  }
}
