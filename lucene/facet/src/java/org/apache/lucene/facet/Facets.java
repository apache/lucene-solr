package org.apache.lucene.facet;

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

import java.io.IOException;
import java.util.List;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollector;

public abstract class Facets {
  /** Returns the topN child labels under the specified
   *  path.  Returns null if the specified path doesn't
   *  exist. */
  public abstract FacetResult getTopChildren(int topN, String dim, String... path) throws IOException;

  /** Return the count for a specific path.  Returns -1 if
   *  this path doesn't exist, else the count. */
  public abstract Number getSpecificValue(String dim, String... path) throws IOException;

  /** Returns topN labels for any dimension that had hits,
   *  sorted by the number of hits that dimension matched;
   *  this is used for "sparse" faceting, where many
   *  different dimensions were indexed depending on the
   *  type of document. */
  public abstract List<FacetResult> getAllDims(int topN) throws IOException;

  // nocommit where to move?

  /** Utility method, to search for top hits by score
   *  ({@link IndexSearcher#search(Query,int)}), but
   *  also collect results into a {@link
   *  FacetsCollector} for faceting. */
  public static TopDocs search(IndexSearcher searcher, Query q, int topN, FacetsCollector sfc) throws IOException {
    // nocommit can we pass the "right" boolean for
    // in-order...?  we'd need access to the protected
    // IS.search methods taking Weight... could use
    // reflection...
    TopScoreDocCollector hitsCollector = TopScoreDocCollector.create(topN, false);
    searcher.search(q, MultiCollector.wrap(hitsCollector, sfc));
    return hitsCollector.topDocs();
  }

  // nocommit where to move?

  /** Utility method, to search for top hits by score with a filter
   *  ({@link IndexSearcher#search(Query,Filter,int)}), but
   *  also collect results into a {@link
   *  FacetsCollector} for faceting. */
  public static TopDocs search(IndexSearcher searcher, Query q, Filter filter, int topN, FacetsCollector sfc) throws IOException {
    if (filter != null) {
      q = new FilteredQuery(q, filter);
    }
    return search(searcher, q, topN, sfc);
  }

  // nocommit where to move?

  /** Utility method, to search for top hits by a custom
   *  {@link Sort} with a filter
   *  ({@link IndexSearcher#search(Query,Filter,int,Sort)}), but
   *  also collect results into a {@link
   *  FacetsCollector} for faceting. */
  public static TopFieldDocs search(IndexSearcher searcher, Query q, Filter filter, int topN, Sort sort, FacetsCollector sfc) throws IOException {
    return search(searcher, q, filter, topN, sort, false, false, sfc);
  }

  // nocommit where to move?

  /** Utility method, to search for top hits by a custom
   *  {@link Sort} with a filter
   *  ({@link IndexSearcher#search(Query,Filter,int,Sort,boolean,boolean)}), but
   *  also collect results into a {@link
   *  FacetsCollector} for faceting. */
  public static TopFieldDocs search(IndexSearcher searcher, Query q, Filter filter, int topN, Sort sort, boolean doDocScores, boolean doMaxScore, FacetsCollector sfc) throws IOException {
    int limit = searcher.getIndexReader().maxDoc();
    if (limit == 0) {
      limit = 1;
    }
    topN = Math.min(topN, limit);

    boolean fillFields = true;
    TopFieldCollector hitsCollector = TopFieldCollector.create(sort, topN,
                                                               null,
                                                               fillFields,
                                                               doDocScores,
                                                               doMaxScore,
                                                               false);
    if (filter != null) {
      q = new FilteredQuery(q, filter);
    }
    searcher.search(q, MultiCollector.wrap(hitsCollector, sfc));
    return (TopFieldDocs) hitsCollector.topDocs();
  }

  // nocommit need searchAfter variants too

}
