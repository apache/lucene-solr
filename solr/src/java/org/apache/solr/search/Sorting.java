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

package org.apache.solr.search;

import org.apache.lucene.search.*;

/**
 * Extra lucene sorting utilities & convenience methods
 *
 *
 *
 */

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
   */
  public static SortField getStringSortField(String fieldName, boolean reverse, boolean nullLast, boolean nullFirst) {
    if (nullLast) {
      if (!reverse) return new SortField(fieldName, nullStringLastComparatorSource);
      else return new SortField(fieldName, SortField.STRING, true);
    } else if (nullFirst) {
      if (reverse) return new SortField(fieldName, nullStringLastComparatorSource, true);
      else return new SortField(fieldName, SortField.STRING, false);
    } else {
      return new SortField(fieldName, SortField.STRING, reverse);
    }
  }


  static final FieldComparatorSource nullStringLastComparatorSource = new MissingStringLastComparatorSource(null);
}

