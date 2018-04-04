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
package org.apache.solr.search;

import org.apache.solr.schema.FieldType;
import org.apache.lucene.search.*;

/**
 * Extra lucene sorting utilities &amp; convenience methods
 *
 *
 * @deprecated custom {@link FieldType}s should use the helper methods in the base class.  Other usage should leverage th underling lucene {@link SortField} classes directly.
 */
@Deprecated
public class Sorting {


  /** Returns a {@link SortField} for a string field.
   *  If nullLast and nullFirst are both false, then default lucene string sorting is used where
   *  null strings sort first in an ascending sort, and last in a descending sort.
   *
   * @param fieldName   the name of the field to sort on
   * @param reverse     true for a reverse (desc) sort
   * @param nullLast    true if null should come last, regardless of sort order
   * @param nullFirst   true if null should come first, regardless of sort order
   * @return SortField
   * @deprecated custom {@link FieldType}s should use {@link FieldType#getSortField}.  Other usage should leverage th underling lucene {@link SortField} classes directly.
   */
  @Deprecated
  public static SortField getStringSortField(String fieldName, boolean reverse, boolean nullLast, boolean nullFirst) {
    SortField sortField = new SortField(fieldName, SortField.Type.STRING, reverse);
    applyMissingFirstLast(sortField, reverse, nullLast, nullFirst);
    return sortField;
  }

  /** Like {@link #getStringSortField}) except safe for tokenized fields
   * @deprecated custom {@link FieldType}s should use {@link FieldType#getSortedSetSortField}.  Other usage should leverage th underling lucene {@link SortedSetSortField} classes directly.
   */
  @Deprecated
  public static SortField getTextSortField(String fieldName, boolean reverse, boolean nullLast, boolean nullFirst) {
    SortField sortField = new SortedSetSortField(fieldName, reverse);
    applyMissingFirstLast(sortField, reverse, nullLast, nullFirst);
    return sortField;
  }
  
  private static void applyMissingFirstLast(SortField in, boolean reverse, boolean nullLast, boolean nullFirst) {
    if (nullFirst && nullLast) {
      throw new IllegalArgumentException("Cannot specify missing values as both first and last");
    }
    
    // 4 cases:
    // missingFirst / forward: default lucene behavior
    // missingFirst / reverse: set sortMissingLast
    // missingLast  / forward: set sortMissingLast
    // missingLast  / reverse: default lucene behavior
    
    if (nullFirst && reverse) {
      in.setMissingValue(SortField.STRING_LAST);
    } else if (nullLast && !reverse) {
      in.setMissingValue(SortField.STRING_LAST);
    }
  }
    
}

