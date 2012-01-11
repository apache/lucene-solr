package org.apache.lucene.index;

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

import org.apache.lucene.index.FieldInfo.IndexOptions;

/** @lucene.experimental */
public interface IndexableFieldType {

  /** True if this field should be indexed (inverted) */
  public boolean indexed();

  /** True if the field's value should be stored */
  public boolean stored();

  /** True if this field's value should be analyzed */
  public boolean tokenized();

  /** True if term vectors should be indexed */
  public boolean storeTermVectors();

  /** True if term vector offsets should be indexed */
  public boolean storeTermVectorOffsets();

  /** True if term vector positions should be indexed */
  public boolean storeTermVectorPositions();

  /** True if norms should not be indexed */
  public boolean omitNorms();

  /** {@link IndexOptions}, describing what should be
   * recorded into the inverted index */
  public IndexOptions indexOptions();

  /** DocValues type; if non-null then the field's value
   *  will be indexed into docValues */
  public DocValues.Type docValueType();
}
