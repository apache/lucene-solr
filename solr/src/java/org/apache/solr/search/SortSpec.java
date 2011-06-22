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

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

/***
 * SortSpec encapsulates a Lucene Sort and a count of the number of documents
 * to return.
 */
public class SortSpec 
{
  Sort sort;
  int num;
  int offset;

  public SortSpec(Sort sort, int num) {
    this(sort,0,num);
  }

  public SortSpec(Sort sort, int offset, int num) {
    this.sort=sort;
    this.offset=offset;
    this.num=num;
  }
  
  public void setSort( Sort s )
  {
    sort = s;
  }

  public static boolean includesScore(Sort sort) {
    if (sort==null) return true;
    for (SortField sf : sort.getSort()) {
      if (sf.getType() == SortField.Type.SCORE) return true;
    }
    return false;
  }

  public boolean includesScore() {
    return includesScore(sort);
  }

  /**
   * Gets the Lucene Sort object, or null for the default sort
   * by score descending.
   */
  public Sort getSort() { return sort; }

  /**
   * Offset into the list of results.
   */
  public int getOffset() { return offset; }

  /**
   * Gets the number of documents to return after sorting.
   *
   * @return number of docs to return, or -1 for no cut off (just sort)
   */
  public int getCount() { return num; }

  @Override
  public String toString() {
    return "start="+offset+ "&rows="+num + (sort==null ? "" : "&sort="+sort); 
  }
}
