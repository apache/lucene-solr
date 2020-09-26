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

  private static FieldType getType(int dimension, VectorValues.ScoreFunction scoreFunction) {
    if (dimension == 0) {
      throw new IllegalArgumentException("cannot index an empty vector");
    }
    if (dimension > VectorValues.MAX_DIMENSIONS) {
      throw new IllegalArgumentException("cannot index vectors with dimension greater than " + VectorValues.MAX_DIMENSIONS);
    }
    FieldType type = new FieldType();
    type.setVectorDimensionsAndScoreFunction(dimension, scoreFunction);
    type.freeze();
    return type;
  }

  public VectorField(String name, float[] vector, VectorValues.ScoreFunction scoreFunction) {
    super(name, getType(vector.length, scoreFunction));
    fieldsData = vector;
  }

  public float[] vectorValue() {
    return (float[]) fieldsData;
  }

  public void setVectorValue(float[] value) {
    if (value.length != type.vectorDimension()) {
      throw new IllegalArgumentException("value length " + value.length + " must match field dimension " + type.vectorDimension());
    }
    fieldsData = value;
  }
}
