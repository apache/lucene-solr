package org.apache.lucene.index.values;

import org.apache.lucene.index.values.DocValues.SortedSource;

/**
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

/**
 * {@link Type} specifies the type of the {@link DocValues} for a certain field.
 * A {@link Type} can specify the actual data type for a field, used compression
 * schemes and high-level data-structures.
 * 
 * @lucene.experimental
 */
public enum Type {

  /**
   * Integral value is stored as packed ints. The bit precision is fixed across
   * the segment, and determined by the min/max values in the field.
   */
  PACKED_INTS,
  /**
   * 32 bit floating point value stored without modification or compression.
   */
  SIMPLE_FLOAT_4BYTE,
  /**
   * 64 bit floating point value stored without modification or compression.
   */
  SIMPLE_FLOAT_8BYTE,

  // TODO(simonw): -- shouldn't lucene decide/detect straight vs
  // deref, as well fixed vs var?
  /**
   * Fixed length straight stored byte variant
   */
  BYTES_FIXED_STRAIGHT,

  /**
   * Fixed length dereferenced (indexed) byte variant
   */
  BYTES_FIXED_DEREF,

  /**
   * Fixed length pre-sorted byte variant
   * 
   * @see SortedSource
   */
  BYTES_FIXED_SORTED,

  /**
   * Variable length straight stored byte variant
   */
  BYTES_VAR_STRAIGHT,

  /**
   * Variable length dereferenced (indexed) byte variant
   */
  BYTES_VAR_DEREF,

  /**
   * Variable length pre-sorted byte variant
   * 
   * @see SortedSource
   */
  BYTES_VAR_SORTED
}
