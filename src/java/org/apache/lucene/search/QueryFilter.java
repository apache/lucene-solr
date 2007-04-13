package org.apache.lucene.search;

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


/** Constrains search results to only match those which also match a provided
 * query.  Results are cached, so that searches after the first on the same
 * index using this filter are much faster.
 *
 * @version $Id$
 * @deprecated use a CachingWrapperFilter with QueryWrapperFilter
 */
public class QueryFilter extends CachingWrapperFilter {

  /** Constructs a filter which only matches documents matching
   * <code>query</code>.
   */
  public QueryFilter(Query query) {
    super(new QueryWrapperFilter(query));
  }

  public boolean equals(Object o) {
    return super.equals((QueryFilter)o);
  }

  public int hashCode() {
    return super.hashCode() ^ 0x923F64B9;  
  }
}
