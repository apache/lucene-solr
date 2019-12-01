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
 * Per-document vector value; {@code float} array with indexed knn graph for fast approximate nearest neighbor search.
 */
public class VectorField extends Field {

  private static FieldType getType(int dimensions, VectorValues.DistanceFunction distFunc) {
    if (dimensions == 0) {
      throw new IllegalArgumentException("VectorField does not support 0 dimensions");
    }
    if (dimensions > VectorValues.MAX_DIMENSIONS) {
      throw new IllegalArgumentException("VectorField does not support greater than " + VectorValues.MAX_DIMENSIONS + " dimensions");
    }
    FieldType type = new FieldType();
    type.setVectorDimensionsAndDistanceFunction(dimensions, distFunc);
    type.freeze();
    return type;
  }

  public VectorField(String name, float[] vector, VectorValues.DistanceFunction distFunc) {
    super(name, VectorValues.encode(vector), getType(vector.length, distFunc));
  }

  /** Changes the value of this field */
  public void setVectorValue(float[] vector) {
    setBytesValue(VectorValues.encode(vector));
  }

}
