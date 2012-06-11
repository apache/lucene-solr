package org.apache.lucene.facet.example.simple;

import java.util.Iterator;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopScoreDocCollector;

import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.facet.example.ExampleUtils;
import org.apache.lucene.facet.index.params.DefaultFacetIndexingParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.search.DrillDown;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.search.params.CountFacetRequest;
import org.apache.lucene.facet.search.params.FacetRequest;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.search.results.FacetResultNode;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

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

/**
 * SampleSearcer searches index with facets. 
 * 
 * @lucene.experimental
 */
public class SimpleSearcher {
  
  /**
   * Search an index with facets.
   * @param indexReader index reader.
   * @param taxoReader taxonomy reader.
   * @throws Exception on error (no detailed exception handling here for sample simplicity
   * @return facet results
   */
  public static List<FacetResult> searchWithFacets (IndexReader indexReader,
      TaxonomyReader taxoReader) throws Exception {
    CountFacetRequest facetRequest = new CountFacetRequest(new CategoryPath("root","a"), 10);
    return searchWithRequest(indexReader, taxoReader, null, facetRequest);
  }
  
  /**
   * Search an index with facets for given facet requests.
   * @param indexReader index reader.
   * @param taxoReader taxonomy reader.
   * @param indexingParams the facet indexing params
   * @param facetRequests facet requests of interest
   * @throws Exception on error (no detailed exception handling here for sample simplicity
   * @return facet results
   */
  public static List<FacetResult> searchWithRequest(IndexReader indexReader,
      TaxonomyReader taxoReader, FacetIndexingParams indexingParams,
      FacetRequest... facetRequests) throws Exception {
    Query q = new TermQuery(new Term(SimpleUtils.TEXT, "white"));
    return searchWithRequestAndQuery(q, indexReader, taxoReader,
        indexingParams, facetRequests);
  }
  
  /**
   * Search an index with facets for given query and facet requests.
   * @param q query of interest
   * @param indexReader index reader.
   * @param taxoReader taxonomy reader.
   * @param indexingParams the facet indexing params
   * @param facetRequests facet requests of interest
   * @throws Exception on error (no detailed exception handling here for sample simplicity
   * @return facet results
   */
  public static List<FacetResult> searchWithRequestAndQuery(Query q,
      IndexReader indexReader, TaxonomyReader taxoReader,
      FacetIndexingParams indexingParams, FacetRequest... facetRequests)
      throws Exception {
    
    ExampleUtils.log("Query: " + q);
    // prepare searcher to search against
    IndexSearcher searcher = new IndexSearcher(indexReader);

    // collect matching documents into a collector
    TopScoreDocCollector topDocsCollector = TopScoreDocCollector.create(10, true);

    if (indexingParams == null) {
      indexingParams = new DefaultFacetIndexingParams();
    }
    
    // Faceted search parameters indicate which facets are we interested in
    FacetSearchParams facetSearchParams = new FacetSearchParams(indexingParams);
    
    // Add the facet requests of interest to the search params
    for (FacetRequest frq : facetRequests) {
      facetSearchParams.addFacetRequest(frq);
    }

    FacetsCollector facetsCollector = new FacetsCollector(facetSearchParams, indexReader, taxoReader);

    // perform documents search and facets accumulation
    searcher.search(q, MultiCollector.wrap(topDocsCollector, facetsCollector));

    // Obtain facets results and print them
    List<FacetResult> res = facetsCollector.getFacetResults();

    int i = 0;
    for (FacetResult facetResult : res) {
      ExampleUtils.log("Res " + (i++) + ": " + facetResult);
    }

    return res;
  }

  /**
   * Search an index with facets drill-down.
   * @param indexReader index reader.
   * @param taxoReader taxonomy reader.
   * @throws Exception on error (no detailed exception handling here for sample simplicity
   * @return facet results
   */
  public static List<FacetResult> searchWithDrillDown(IndexReader indexReader,
      TaxonomyReader taxoReader) throws Exception {

    // base query the user is interested in
    Query baseQuery = new TermQuery(new Term(SimpleUtils.TEXT, "white"));

    // facet of interest
    CountFacetRequest facetRequest = new CountFacetRequest(new CategoryPath("root","a"), 10);
    
    // initial search - all docs matching the base query will contribute to the accumulation 
    List<FacetResult> res1 = searchWithRequest(indexReader, taxoReader, null, facetRequest);
    
    // a single result (because there was a single request) 
    FacetResult fres = res1.get(0);
    
    // assume the user is interested in the second sub-result
    // (just take the second sub-result returned by the iterator - we know there are 3 results!)
    Iterator<? extends FacetResultNode> resIterator = fres.getFacetResultNode().getSubResults().iterator();
    resIterator.next(); // skip first result
    CategoryPath categoryOfInterest = resIterator.next().getLabel();
    
    // drill-down preparation: turn the base query into a drill-down query for the category of interest
    Query q2 = DrillDown.query(baseQuery, categoryOfInterest);
    
    // that's it - search with the new query and we're done!
    // only documents both matching the base query AND containing the 
    // category of interest will contribute to the new accumulation
    return searchWithRequestAndQuery(q2, indexReader, taxoReader, null, facetRequest);
  }
  
}
