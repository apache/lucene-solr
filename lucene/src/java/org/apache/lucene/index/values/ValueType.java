package org.apache.lucene.index.values;

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

import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.PerDocConsumer;
import org.apache.lucene.index.values.DocValues.SortedSource;

/**
 * {@link ValueType} specifies the type of the {@link DocValues} for a certain field.
 * A {@link ValueType} only defines the data type for a field while the actual
 * Implementation used to encode and decode the values depends on the field's
 * {@link Codec}. It is up to the {@link Codec} implementing
 * {@link PerDocConsumer#addValuesField(org.apache.lucene.index.FieldInfo)} and
 * using a different low-level implementations to write the stored values for a
 * field.
 * 
 * @lucene.experimental
 */
public enum ValueType {
  /*
   * TODO: Add INT_32 INT_64 INT_16 & INT_8?!
   */
  /**
   * Integer values.
   */
  INTS,
   
  /**
   * 32 bit floating point values.
   */
  FLOAT_32,
  /**
   * 64 bit floating point values.
   */
  FLOAT_64,

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
