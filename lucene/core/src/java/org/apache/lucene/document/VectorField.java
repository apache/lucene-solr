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

/**
 * Per-document vector value; {@code float} array.
 */
public class VectorField extends Field {

  private static FieldType getType(float[] v, VectorValues.ScoreFunction scoreFunction) {
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
    if (scoreFunction == null) {
      throw new IllegalArgumentException("score function must not be null");
    }
    FieldType type = new FieldType();
    type.setVectorDimensionsAndScoreFunction(dimension, scoreFunction);
    type.freeze();
    return type;
  }

  /** Creates a numeric vector field. Fields are single-valued: each document has either one value
   * or no value. Vectors of a single field share the same dimension and score function.
   *
   *  @param name field name
   *  @param vector value
   *  @param scoreFunction a function defining vector proximity.
   *  @throws IllegalArgumentException if any parameter is null, or the vector is empty or has dimension &gt; 1024.
   */
  public VectorField(String name, float[] vector, VectorValues.ScoreFunction scoreFunction) {
    super(name, getType(vector, scoreFunction));
    fieldsData = vector;
  }

  /** Creates a numeric vector field with the default EUCLIDEAN (L2) score function. Fields are
   * single-valued: each document has either one value or no value. Vectors of a single field share
   * the same dimension and score function.
   *
   *  @param name field name
   *  @param vector value
   *  @throws IllegalArgumentException if any parameter is null, or the vector is empty or has dimension &gt; 1024.
   */
  public VectorField(String name, float[] vector) {
    this(name, vector, VectorValues.ScoreFunction.EUCLIDEAN);
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
    if (value.length != type.vectorDimension()) {
      throw new IllegalArgumentException("value length " + value.length + " must match field dimension " + type.vectorDimension());
    }
    fieldsData = value;
  }
}
