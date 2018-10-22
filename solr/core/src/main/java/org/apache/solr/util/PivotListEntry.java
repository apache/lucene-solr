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
package org.apache.solr.util;

import org.apache.solr.common.util.NamedList;

import java.util.Locale;

/**
 * Enum for modeling the elements of a (nested) pivot entry as expressed in a NamedList
 */
public enum PivotListEntry {
  
  // mandatory entries with exact indexes
  FIELD(0),
  VALUE(1),
  COUNT(2),
  // optional entries
  PIVOT,
  STATS,
  QUERIES,
  RANGES;
  
  private static final int MIN_INDEX_OF_OPTIONAL = 3;

  /** 
   * Given a NamedList representing a Pivot Value, this is Minimum Index at 
   * which this PivotListEntry may exist 
   */
  private final int minIndex;
  
  private PivotListEntry() {
    this.minIndex = MIN_INDEX_OF_OPTIONAL;
  }
  private PivotListEntry(int minIndex) {
    assert minIndex < MIN_INDEX_OF_OPTIONAL;
    this.minIndex = minIndex;
  }
  
  /**
   * Case-insensitive lookup of PivotListEntry by name
   * @see #getName
   */
  public static PivotListEntry get(String name) {
    return PivotListEntry.valueOf(name.toUpperCase(Locale.ROOT));
  }

  /**
   * Name of this entry when used in response
   * @see #get
   */
  public String getName() {
    return name().toLowerCase(Locale.ROOT);
  }
  
  /**
   * Given a {@link NamedList} representing a Pivot Value, extracts the Object 
   * which corrisponds to this {@link PivotListEntry}, or returns null if not found.
   */
  public Object extract(NamedList<Object> pivotList) {
    if (this.minIndex < MIN_INDEX_OF_OPTIONAL) {
      // a mandatory entry at an exact index.
      assert this.getName().equals(pivotList.getName(this.minIndex));
      assert this.minIndex < pivotList.size();
      return pivotList.getVal(this.minIndex);
    }
    // otherwise...
    // scan starting at the min/optional index
    return pivotList.get(this.getName(), this.minIndex);
  }

}
