package org.apache.lucene.search;

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

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.util.HalfFloat;
import org.apache.lucene.util.NumericUtils;

public class HalfFloatComparator extends NumericComparator<Float> {

  /** 
   * Creates a new comparator based on {@link Float#compare} for {@code numHits}.
   * When a document has no value for the field, {@code missingValue} is substituted. 
   */
  public HalfFloatComparator(int numHits, String field, float missingValue) {
    super(numHits, field, NumericUtils.halfFloatToShort(missingValue));
  }
    
  @Override
  protected Float longToValue(long value) {
    return NumericUtils.shortToHalfFloat((short) value);
  }

  @Override
  protected long valueToLong(Float value) {
    return NumericUtils.halfFloatToShort(value);
  }
}
