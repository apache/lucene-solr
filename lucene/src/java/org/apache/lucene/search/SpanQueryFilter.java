package org.apache.lucene.search;
/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.OpenBitSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Constrains search results to only match those which also match a provided
 * query. Also provides position information about where each document matches
 * at the cost of extra space compared with the QueryWrapperFilter.
 * There is an added cost to this above what is stored in a {@link QueryWrapperFilter}.  Namely,
 * the position information for each matching document is stored.
 * <p/>
 * This filter does not cache.  See the {@link org.apache.lucene.search.CachingSpanFilter} for a wrapper that
 * caches.
 */
public class SpanQueryFilter extends SpanFilter {
  protected SpanQuery query;

  protected SpanQueryFilter()
  {
    
  }

  /** Constructs a filter which only matches documents matching
   * <code>query</code>.
   * @param query The {@link org.apache.lucene.search.spans.SpanQuery} to use as the basis for the Filter.
   */
  public SpanQueryFilter(SpanQuery query) {
    this.query = query;
  }

  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context) throws IOException {
    SpanFilterResult result = bitSpans(context);
    return result.getDocIdSet();
  }

  @Override
  public SpanFilterResult bitSpans(AtomicReaderContext context) throws IOException {

    final OpenBitSet bits = new OpenBitSet(context.reader.maxDoc());
    Spans spans = query.getSpans(context);
    List<SpanFilterResult.PositionInfo> tmp = new ArrayList<SpanFilterResult.PositionInfo>(20);
    int currentDoc = -1;
    SpanFilterResult.PositionInfo currentInfo = null;
    while (spans.next())
    {
      int doc = spans.doc();
      bits.set(doc);
      if (currentDoc != doc)
      {
        currentInfo = new SpanFilterResult.PositionInfo(doc);
        tmp.add(currentInfo);
        currentDoc = doc;
      }
      currentInfo.addPosition(spans.start(), spans.end());
    }
    return new SpanFilterResult(bits, tmp);
  }


  public SpanQuery getQuery() {
    return query;
  }

  @Override
  public String toString() {
    return "SpanQueryFilter(" + query + ")";
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof SpanQueryFilter && this.query.equals(((SpanQueryFilter) o).query);
  }

  @Override
  public int hashCode() {
    return query.hashCode() ^ 0x923F64B9;
  }
}
