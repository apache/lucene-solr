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
package org.apache.solr.schema;

import org.apache.lucene.search.SortField;

public enum NumberType {
  INTEGER(SortField.Type.INT, Integer.MIN_VALUE, Integer.MAX_VALUE),
  LONG(SortField.Type.LONG, Long.MIN_VALUE, Long.MAX_VALUE),
  FLOAT(SortField.Type.FLOAT, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY),
  DOUBLE(SortField.Type.DOUBLE, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY),
  DATE(SortField.Type.LONG, Long.MIN_VALUE, Long.MAX_VALUE);

  /** The SortField type that corrisponds with this NumberType */
  public final SortField.Type sortType;
  /** 
   * The effective value to use when sorting on this field should result in docs w/o a value 
   * sorting "low" (which may be "first" or "last" depending on sort direction) 
   * @see SortField#setMissingValue
   */
  public final Object sortMissingLow;
  /** 
   * The effective value to use when sorting on this field should result in docs w/o a value 
   * sorting "low" (which may be "first" or "last" depending on sort direction) 
   * @see SortField#setMissingValue
   */
  public final Object sortMissingHigh;
  
  private NumberType(SortField.Type sortType, Object sortMissingLow, Object sortMissingHigh) {
    this.sortType = sortType;
    this.sortMissingLow = sortMissingLow;
    this.sortMissingHigh = sortMissingHigh;

  }
}
