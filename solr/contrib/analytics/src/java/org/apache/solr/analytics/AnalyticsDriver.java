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
package org.apache.solr.analytics;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.solr.analytics.AnalyticsRequestManager.StreamingInfo;
import org.apache.solr.analytics.facet.AbstractSolrQueryFacet.FacetValueQueryExecuter;
import org.apache.solr.analytics.facet.StreamingFacet;
import org.apache.solr.analytics.function.ReductionCollectionManager;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.Filter;
import org.apache.solr.search.SolrIndexSearcher;

public class AnalyticsDriver {

  /**
   * Drive the collection of reduction data. This includes overall data as well as faceted data.
   *
   * @param manager of the request to drive
   * @param searcher the results of the query
   * @param filter that represents the overall query
   * @param queryRequest used for the search request
   * @throws IOException if an error occurs while reading from Solr
   */
  public static void drive(AnalyticsRequestManager manager, SolrIndexSearcher searcher, Filter filter, SolrQueryRequest queryRequest) throws IOException {
    StreamingInfo streamingInfo = manager.getStreamingFacetInfo();
    Iterable<StreamingFacet> streamingFacets = streamingInfo.streamingFacets;
    ReductionCollectionManager collectionManager = streamingInfo.streamingCollectionManager;

    Iterable<FacetValueQueryExecuter> facetExecuters = manager.getFacetExecuters(filter, queryRequest);

    // Streaming phase (Overall results & Value/Pivot Facets)
    // Loop through all documents and collect reduction data for streaming facets and overall results
    if (collectionManager.needsCollection()) {
      List<LeafReaderContext> contexts = searcher.getTopReaderContext().leaves();
      for (int leafNum = 0; leafNum < contexts.size(); leafNum++) {
        LeafReaderContext context = contexts.get(leafNum);
        DocIdSet dis = filter.getDocIdSet(context, null); // solr docsets already exclude any deleted docs
        if (dis == null) {
          continue;
        }
        DocIdSetIterator disi = dis.iterator();
        if (disi != null) {
          collectionManager.doSetNextReader(context);
          int doc = disi.nextDoc();
          while( doc != DocIdSetIterator.NO_MORE_DOCS){
            // Add a document to the statistics being generated
            collectionManager.collect(doc);
            streamingFacets.forEach( facet -> facet.addFacetValueCollectionTargets() );
            collectionManager.apply();
            doc = disi.nextDoc();
          }
        }
      }
    }

    // Executing phase (Query/Range Facets)
    // Send additional Solr Queries to compute facet values
    for (FacetValueQueryExecuter executer : facetExecuters) {
      executer.execute(searcher);
    }
  }
}
