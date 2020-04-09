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
package org.apache.lucene.queries.function.valuesource.vectors;

import org.apache.lucene.queries.function.ValueSource;

public class FloatVectorDotProductFunction extends FloatVectorFunction {

  public FloatVectorDotProductFunction(String queryVector, ValueSource denseVectorFieldValueSource, Selector selector) {
    super(queryVector, denseVectorFieldValueSource, selector);
  }

  public FloatVectorDotProductFunction(String queryVector, ValueSource denseVectorFieldValueSource) {
    this(queryVector, denseVectorFieldValueSource, Selector.MAX);
  }

  protected float func(float[] vectorA, float[] vectorB){
      double dotProduct = 0.0;
      for (int i = 0; i < vectorA.length; i++) {
        dotProduct += vectorA[i] * vectorB[i];
      }
      return (float) dotProduct;
  }

  public static final String NAME = "vector_dotproduct";

  @Override
  public String description() {
    return NAME;
  } //TODO: Useful description, but not too large


}
