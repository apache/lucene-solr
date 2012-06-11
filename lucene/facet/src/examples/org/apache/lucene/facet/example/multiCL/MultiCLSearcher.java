package org.apache.lucene.facet.example.multiCL;

import java.util.List;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;

import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.facet.example.ExampleUtils;
import org.apache.lucene.facet.example.simple.SimpleUtils;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.search.params.CountFacetRequest;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;

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
 * MultiSearcher searches index with facets over an index with multiple
 * category lists.
 * 
 * @lucene.experimental
 */
public class MultiCLSearcher {

  /**
   * Search an index with facets.
   * 
   * @param indexDir
   *            Directory of the search index.
   * @param taxoDir
   *            Directory of the taxonomy index.
   * @throws Exception
   *             on error (no detailed exception handling here for sample
   *             simplicity
   * @return facet results
   */
  public static List<FacetResult> searchWithFacets(Directory indexDir,
      Directory taxoDir, FacetIndexingParams iParams) throws Exception {
    
    // prepare index reader and taxonomy.
    IndexReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxo = new DirectoryTaxonomyReader(taxoDir);
    
    // Get results
    List<FacetResult> results = searchWithFacets(indexReader, taxo, iParams);
    
    // we're done, close the index reader and the taxonomy.
    indexReader.close();
    taxo.close();
    return results;
  }
  
  public static List<FacetResult> searchWithFacets(IndexReader indexReader,
      TaxonomyReader taxo, FacetIndexingParams iParams) throws Exception {
    // prepare searcher to search against
    IndexSearcher searcher = new IndexSearcher(indexReader);

    // faceted search is working in 2 steps:
    // 1. collect matching documents
    // 2. aggregate facets for collected documents and
    // generate the requested faceted results from the aggregated facets

    // step 1: create a query for finding matching documents for which we
    // accumulate facets
    Query q = new TermQuery(new Term(SimpleUtils.TEXT, "Quis"));
    ExampleUtils.log("Query: " + q);

    TopScoreDocCollector topDocsCollector = TopScoreDocCollector.create(10,
        true);

    // Faceted search parameters indicate which facets are we interested in
    FacetSearchParams facetSearchParams = new FacetSearchParams(iParams);
    facetSearchParams.addFacetRequest(new CountFacetRequest(
        new CategoryPath("5"), 10));
    facetSearchParams.addFacetRequest(new CountFacetRequest(
        new CategoryPath("5", "5"), 10));
    facetSearchParams.addFacetRequest(new CountFacetRequest(
        new CategoryPath("6", "2"), 10));

    // Facets collector is the simplest interface for faceted search.
    // It provides faceted search functions that are sufficient to many
    // application,
    // although it is insufficient for tight control on faceted search
    // behavior - in those
    // situations other, more low-level interfaces are available, as
    // demonstrated in other search examples.
    FacetsCollector facetsCollector = new FacetsCollector(
        facetSearchParams, indexReader, taxo);

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

}
