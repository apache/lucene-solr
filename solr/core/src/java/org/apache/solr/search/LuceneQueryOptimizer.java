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

/* Copyright (c) 2003 The Nutch Organization.  All rights reserved.   */
/* Use subject to the conditions in http://www.nutch.org/LICENSE.txt. */


import org.apache.lucene.search.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.io.IOException;

/** Utility which converts certain query clauses into {@link QueryFilter}s and
 * caches these.  Only required {@link TermQuery}s whose boost is zero and
 * whose term occurs in at least a certain fraction of documents are converted
 * to cached filters.  This accelerates query constraints like language,
 * document format, etc., which do not affect ranking but might otherwise slow
 * search considerably. */
// Taken from Nutch and modified - YCS
class LuceneQueryOptimizer {
  private LinkedHashMap cache;                   // an LRU cache of QueryFilter

  private float threshold;

  /** Construct an optimizer that caches and uses filters for required {@link
   * TermQuery}s whose boost is zero.
   * @param cacheSize the number of QueryFilters to cache
   * @param threshold the fraction of documents which must contain term
   */
  public LuceneQueryOptimizer(final int cacheSize, float threshold) {
    this.cache = new LinkedHashMap(cacheSize, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
          return size() > cacheSize;              // limit size of cache
        }
      };
    this.threshold = threshold;
  }

  public TopDocs optimize(BooleanQuery original,
                          IndexSearcher searcher,
                          int numHits,
                          Query[] queryOut,
                          Filter[] filterOut
                          )
    throws IOException {

    BooleanQuery query = new BooleanQuery();
    BooleanQuery filterQuery = null;

    for (BooleanClause c : original.clauses()) {

/***
System.out.println("required="+c.required);
System.out.println("boost="+c.query.getBoost());
System.out.println("isTermQuery="+(c.query instanceof TermQuery));
if (c.query instanceof TermQuery) {
 System.out.println("term="+((TermQuery)c.query).getTerm());
 System.out.println("docFreq="+searcher.docFreq(((TermQuery)c.query).getTerm()));
}
***/
      Query q = c.getQuery();
      if (c.isRequired()                              // required
          && q.getBoost() == 0.0f           // boost is zero
          && q instanceof TermQuery         // TermQuery
          && (searcher.docFreq(((TermQuery)q).getTerm())
              / (float)searcher.maxDoc()) >= threshold) { // check threshold
        if (filterQuery == null)
          filterQuery = new BooleanQuery();
        filterQuery.add(q, BooleanClause.Occur.MUST);    // filter it
//System.out.println("WooHoo... qualified to be hoisted to a filter!");
      } else {
        query.add(c);                             // query it
      }
    }

    Filter filter = null;
    if (filterQuery != null) {
      synchronized (cache) {                      // check cache
        filter = (Filter)cache.get(filterQuery);
      }
      if (filter == null) {                       // miss
        filter = new CachingWrapperFilter(new QueryWrapperFilter(filterQuery)); // construct new entry
        synchronized (cache) {
          cache.put(filterQuery, filter);         // cache it
        }
      }        
    }

    // YCS: added code to pass out optimized query and filter
    // so they can be used with Hits
    if (queryOut != null && filterOut != null) {
      queryOut[0] = query; filterOut[0] = filter;
      return null;
    } else {
      return searcher.search(query, filter, numHits);
    }

  }
}
