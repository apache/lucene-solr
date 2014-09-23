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

package org.apache.solr.analytics.request;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.solr.analytics.accumulator.BasicAccumulator;
import org.apache.solr.analytics.accumulator.FacetingAccumulator;
import org.apache.solr.analytics.accumulator.ValueAccumulator;
import org.apache.solr.analytics.plugin.AnalyticsStatisticsCollector;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class which computes the set of {@link AnalyticsRequest}s.
 */
public class AnalyticsStats {
  protected DocSet docs;
  protected SolrParams params;
  protected SolrIndexSearcher searcher;
  protected SolrQueryRequest req;
  protected AnalyticsStatisticsCollector statsCollector;
  private static final Logger log = LoggerFactory.getLogger(AnalyticsStats.class);
  
  public AnalyticsStats(SolrQueryRequest req, DocSet docs, SolrParams params, AnalyticsStatisticsCollector statsCollector) {
    this.req = req;
    this.searcher = req.getSearcher();
    this.docs = docs;
    this.params = params; 
    this.statsCollector = statsCollector;
  }

  /**
   * Calculates the analytics requested in the Parameters.
   * 
   * @return List of results formated to mirror the input XML.
   * @throws IOException if execution fails
   */
  public NamedList<?> execute() throws IOException {
    statsCollector.startRequest();
    NamedList<Object> res = new NamedList<>();
    List<AnalyticsRequest> requests;
    
    requests = AnalyticsRequestFactory.parse(searcher.getSchema(), params);

    if(requests == null || requests.size()==0){
      return res;
    }
    statsCollector.addRequests(requests.size());
    
    // Get filter to all docs
    Filter filter = docs.getTopFilter();
    
    // Computing each Analytics Request Seperately
    for( AnalyticsRequest areq : requests ){
      // The Accumulator which will control the statistics generation
      // for the entire analytics request
      ValueAccumulator accumulator; 
      
      // The number of total facet requests
      int facets = areq.getFieldFacets().size()+areq.getRangeFacets().size()+areq.getQueryFacets().size();
      try {
        if( facets== 0 ){
          accumulator = BasicAccumulator.create(searcher, docs, areq);
        } else {
          accumulator = FacetingAccumulator.create(searcher, docs, areq, req);
        }
      } catch (IOException e) {
        log.warn("Analytics request '"+areq.getName()+"' failed", e);
        continue;
      }

      statsCollector.addStatsCollected(((BasicAccumulator)accumulator).getNumStatsCollectors());
      statsCollector.addStatsRequests(areq.getExpressions().size());
      statsCollector.addFieldFacets(areq.getFieldFacets().size());
      statsCollector.addRangeFacets(areq.getRangeFacets().size());
      statsCollector.addQueryFacets(areq.getQueryFacets().size());
      statsCollector.addQueries(((BasicAccumulator)accumulator).getNumQueries());
      
      // Loop through the documents returned by the query and add to accumulator
      List<LeafReaderContext> contexts = searcher.getTopReaderContext().leaves();
      for (int leafNum = 0; leafNum < contexts.size(); leafNum++) {
        LeafReaderContext context = contexts.get(leafNum);
        DocIdSet dis = filter.getDocIdSet(context, null); // solr docsets already exclude any deleted docs
        DocIdSetIterator disi = null;
        if (dis != null) {
          disi = dis.iterator();
        }

        if (disi != null) {
          accumulator.getLeafCollector(context);
          int doc = disi.nextDoc();
          while( doc != DocIdSetIterator.NO_MORE_DOCS){
            // Add a document to the statistics being generated
            accumulator.collect(doc);
            doc = disi.nextDoc();
          }
        }
      }
      
      // do some post-processing
      accumulator.postProcess();
     
      // compute the stats
      accumulator.compute();
      
      res.add(areq.getName(),accumulator.export());
    }

    statsCollector.endRequest();
    return res;
  }
}
