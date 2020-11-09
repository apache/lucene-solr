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

package org.apache.lucene.index;

import java.io.IOException;

import org.apache.lucene.util.BytesRef;

/**
 * Provides random access to vectors by dense ordinal.
 *
 * @lucene.experimental
 */
public interface RandomAccessVectorValues {

  /**
   * Return the number of vector values
   */
  int size();

  /**
   * Return the dimension of the returned vector values
   */
  int dimension();

  /**
   * Return the search strategy used to compare these vectors
   */
  VectorValues.SearchStrategy searchStrategy();

  /**
   * Return the vector value indexed at the given ordinal. The provided floating point array may
   * be shared and overwritten by subsequent calls to this method and {@link #binaryValue(int)}.
   * @param targetOrd a valid ordinal, &ge; 0 and &lt; {@link #size()}.
   */
  float[] vectorValue(int targetOrd) throws IOException;

  /**
   * Return the vector indexed at the given ordinal value as an array of bytes in a BytesRef;
   * these are the bytes corresponding to the float array. The provided bytes may be shared and overwritten
   * by subsequent calls to this method and {@link #vectorValue(int)}.
   * @param targetOrd a valid ordinal, &ge; 0 and &lt; {@link #size()}.
   */
  BytesRef binaryValue(int targetOrd) throws IOException;
}
