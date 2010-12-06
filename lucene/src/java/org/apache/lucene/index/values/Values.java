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

/** Controls whether per-field values are stored into
 *  index.  This storage is non-sparse, so it's best to
 *  use this when all docs have the field, and loads all
 *  values into RAM, exposing a random access API, when
 *  loaded.
 *
 * @lucene.experimental 
 */
public enum Values {

  /** Integral value is stored as packed ints.  The bit
   *  precision is fixed across the segment, and
   *  determined by the min/max values in the field. */
  PACKED_INTS,
  SIMPLE_FLOAT_4BYTE,
  SIMPLE_FLOAT_8BYTE,

  // TODO(simonw): -- shouldn't lucene decide/detect straight vs
  // deref, as well fixed vs var?
  BYTES_FIXED_STRAIGHT,
  BYTES_FIXED_DEREF,
  BYTES_FIXED_SORTED,

  BYTES_VAR_STRAIGHT,
  BYTES_VAR_DEREF,
  BYTES_VAR_SORTED

  // TODO(simonw): -- need STRING variants as well
}
