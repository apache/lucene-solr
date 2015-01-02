package org.apache.lucene.search;

import org.apache.lucene.util.NumericUtils;

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

/** Parses field's values as float (using {@link
 *  org.apache.lucene.index.LeafReader#getNumericDocValues(String)} and sorts by ascending value */
public class FloatComparator extends NumericComparator<Float> {

  /** 
   * Creates a new comparator based on {@link Float#compare} for {@code numHits}.
   * When a document has no value for the field, {@code missingValue} is substituted. 
   */
  public FloatComparator(int numHits, String field, Float missingValue) {
    super(numHits, field, NumericUtils.floatToInt(missingValue));
  }

  @Override
  protected Float longToValue(long value) {
    return NumericUtils.intToFloat((int) value);
  }

  @Override
  protected long valueToLong(Float value) {
    return NumericUtils.floatToInt(value);
  }
}

