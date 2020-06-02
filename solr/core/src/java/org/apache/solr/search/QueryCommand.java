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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;

/**
 * A query request command to avoid having to change the method signatures if we want to pass additional information
 * to the searcher.
 */
public class QueryCommand {
  
  private Query query;
  private List<Query> filterList;
  private DocSet filter;
  private Sort sort;
  private int offset;
  private int len;
  private int supersetMaxDoc;
  private int flags;
  private long timeAllowed = -1;
  private int minExactCount = Integer.MAX_VALUE;
  private CursorMark cursorMark;
  
  public CursorMark getCursorMark() {
    return cursorMark;
  }
  
  public QueryCommand setCursorMark(CursorMark cursorMark) {
    this.cursorMark = cursorMark;
    if (null != cursorMark) {
      // If we're using a cursor then we can't allow queryResult caching because the
      // cache keys don't know anything about the collector used.
      //
      // in theory, we could enhance the cache keys to be aware of the searchAfter
      // FieldDoc but then there would still be complexity around things like the cache
      // window size that would need to be worked out
      //
      // we *can* however allow the use of checking the filterCache for non-score based
      // sorts, because that still runs our paging collector over the entire DocSet
      this.flags |= (SolrIndexSearcher.NO_CHECK_QCACHE | SolrIndexSearcher.NO_SET_QCACHE);
    }
    return this;
  }
  
  public Query getQuery() {
    return query;
  }
  
  public QueryCommand setQuery(Query query) {
    this.query = query;
    return this;
  }
  
  public List<Query> getFilterList() {
    return filterList;
  }
  
  /**
   * @throws IllegalArgumentException
   *           if filter is not null.
   */
  public QueryCommand setFilterList(List<Query> filterList) {
    if (filter != null) {
      throw new IllegalArgumentException("Either filter or filterList may be set in the QueryCommand, but not both.");
    }
    this.filterList = filterList;
    return this;
  }
  
  /**
   * A simple setter to build a filterList from a query
   * 
   * @throws IllegalArgumentException
   *           if filter is not null.
   */
  public QueryCommand setFilterList(Query f) {
    if (filter != null) {
      throw new IllegalArgumentException("Either filter or filterList may be set in the QueryCommand, but not both.");
    }
    filterList = null;
    if (f != null) {
      filterList = new ArrayList<>(2);
      filterList.add(f);
    }
    return this;
  }
  
  public DocSet getFilter() {
    return filter;
  }
  
  /**
   * @throws IllegalArgumentException
   *           if filterList is not null.
   */
  public QueryCommand setFilter(DocSet filter) {
    if (filterList != null) {
      throw new IllegalArgumentException("Either filter or filterList may be set in the QueryCommand, but not both.");
    }
    this.filter = filter;
    return this;
  }
  
  public Sort getSort() {
    return sort;
  }
  
  public QueryCommand setSort(Sort sort) {
    this.sort = sort;
    return this;
  }
  
  public int getOffset() {
    return offset;
  }
  
  public QueryCommand setOffset(int offset) {
    this.offset = offset;
    return this;
  }
  
  public int getLen() {
    return len;
  }
  
  public QueryCommand setLen(int len) {
    this.len = len;
    return this;
  }
  
  public int getSupersetMaxDoc() {
    return supersetMaxDoc;
  }
  
  public QueryCommand setSupersetMaxDoc(int supersetMaxDoc) {
    this.supersetMaxDoc = supersetMaxDoc;
    return this;
  }
  
  public int getFlags() {
    return flags;
  }
  
  public QueryCommand replaceFlags(int flags) {
    this.flags = flags;
    return this;
  }
  
  public QueryCommand setFlags(int flags) {
    this.flags |= flags;
    return this;
  }
  
  public QueryCommand clearFlags(int flags) {
    this.flags &= ~flags;
    return this;
  }
  
  public long getTimeAllowed() {
    return timeAllowed;
  }
  
  public QueryCommand setTimeAllowed(long timeAllowed) {
    this.timeAllowed = timeAllowed;
    return this;
  }

  public int getMinExactCount() {
    return minExactCount;
  }

  public QueryCommand setMinExactCount(int count) {
    this.minExactCount = count;
    return this;
  }
  
  public boolean isNeedDocSet() {
    return (flags & SolrIndexSearcher.GET_DOCSET) != 0;
  }
  
  public QueryCommand setNeedDocSet(boolean needDocSet) {
    if (needDocSet) {
      return setFlags(SolrIndexSearcher.GET_DOCSET);
    } else {
      return clearFlags(SolrIndexSearcher.GET_DOCSET);
    }
  }
  
  public boolean getTerminateEarly() {
    return (flags & SolrIndexSearcher.TERMINATE_EARLY) != 0;
  }

  public QueryCommand setTerminateEarly(boolean segmentTerminateEarly) {
    if (segmentTerminateEarly) {
      return setFlags(SolrIndexSearcher.TERMINATE_EARLY);
    } else {
      return clearFlags(SolrIndexSearcher.TERMINATE_EARLY);
    }
  }

  public boolean getSegmentTerminateEarly() {
    return (flags & SolrIndexSearcher.SEGMENT_TERMINATE_EARLY) != 0;
  }

  public QueryCommand setSegmentTerminateEarly(boolean segmentSegmentTerminateEarly) {
    if (segmentSegmentTerminateEarly) {
      return setFlags(SolrIndexSearcher.SEGMENT_TERMINATE_EARLY);
    } else {
      return clearFlags(SolrIndexSearcher.SEGMENT_TERMINATE_EARLY);
    }
  }

}
