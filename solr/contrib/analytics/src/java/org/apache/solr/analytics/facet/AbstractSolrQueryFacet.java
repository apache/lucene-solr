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
package org.apache.solr.analytics.facet;

import java.io.IOException;
import java.util.function.Consumer;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.solr.analytics.AnalyticsDriver;
import org.apache.solr.analytics.function.ReductionCollectionManager.ReductionDataCollection;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.Filter;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Solr Query Facets are AnalyticsFacets that are calculated after the document streaming phase has occurred in the {@link AnalyticsDriver}
 * (during which StreamingFacets and overall expressions are calculated). {@link AbstractSolrQueryFacet}s should not be confused with {@link QueryFacet}s,
 * which are a specific sub-type.
 *
 * <p>
 * The filtering for these facets is done through issuing additional Solr queries, and collecting on the resulting documents.
 * Unlike streaming facets, which have an unspecified amount of facet values (facet buckets), the amount of facet values is determined by the user and
 * a Solr query is issued for each requested facet value.
 */
public abstract class AbstractSolrQueryFacet extends AnalyticsFacet {

  protected AbstractSolrQueryFacet(String name) {
    super(name);
  }

  /**
   * Returns the set of {@link FacetValueQueryExecuter}s, one for each facet value, through the given consumer.
   *
   * Each of these executors will be executed after the streaming phase in the {@link AnalyticsDriver}.
   *
   * @param filter the overall filter representing the documents being used for the analytics request
   * @param queryRequest the queryRequest
   * @param consumer the consumer of each facet value's executer
   */
  public abstract void createFacetValueExecuters(final Filter filter, SolrQueryRequest queryRequest, Consumer<FacetValueQueryExecuter> consumer);

  /**
   * This executer is in charge of issuing the Solr query for a facet value and collecting results as the query is processed.
   */
  public class FacetValueQueryExecuter extends SimpleCollector {
    private final ReductionDataCollection collection;
    private final Query query;

    /**
     * Create an executer to collect the given reduction data from the given Solr query.
     *
     * @param collection The reduction data to collect while querying
     * @param query The query used to filter for the facet value
     */
    public FacetValueQueryExecuter(ReductionDataCollection collection, Query query) {
      this.collection = collection;
      this.query = query;
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
      collectionManager.doSetNextReader(context);
    }

    @Override
    public void collect(int doc) throws IOException {
      collectionManager.collect(doc);
      collectionManager.apply();
    }

    /**
     * Start the collection for this facet value.
     *
     * @param searcher the solr searcher
     * @throws IOException if an exception occurs during the querying
     */
    public void execute(SolrIndexSearcher searcher) throws IOException {
      collectionManager.clearLastingCollectTargets();
      collectionManager.addLastingCollectTarget(collection);
      searcher.search(query, this);
    }
  }
}
