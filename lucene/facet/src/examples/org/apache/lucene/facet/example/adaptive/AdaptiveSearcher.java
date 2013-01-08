package org.apache.lucene.facet.example.adaptive;

import java.util.List;

import org.apache.lucene.facet.example.ExampleUtils;
import org.apache.lucene.facet.example.simple.SimpleUtils;
import org.apache.lucene.facet.search.AdaptiveFacetsAccumulator;
import org.apache.lucene.facet.search.ScoredDocIdCollector;
import org.apache.lucene.facet.search.params.CountFacetRequest;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;

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
 * Search with facets through the {@link AdaptiveFacetsAccumulator} 
 * 
 * @lucene.experimental
 */
public class AdaptiveSearcher {
  
  /**
   * Search with facets through the {@link AdaptiveFacetsAccumulator} 
   * @param indexDir Directory of the search index.
   * @param taxoDir Directory of the taxonomy index.
   * @throws Exception on error (no detailed exception handling here for sample simplicity
   * @return facet results
   */
  public static List<FacetResult> searchWithFacets (Directory indexDir, Directory taxoDir) throws Exception {
    // prepare index reader and taxonomy.
    TaxonomyReader taxo = new DirectoryTaxonomyReader(taxoDir);
    IndexReader indexReader = DirectoryReader.open(indexDir);
    
    // prepare searcher to search against
    IndexSearcher searcher = new IndexSearcher(indexReader);
    
    // faceted search is working in 2 steps: 
    // 1. collect matching documents
    // 2. aggregate facets for collected documents and
    //    generate the requested faceted results from the aggregated facets
    
    // step 1: collect matching documents into a collector
    Query q = new TermQuery(new Term(SimpleUtils.TEXT,"white"));
    ExampleUtils.log("Query: "+q);
    
    // regular collector for scoring matched documents
    TopScoreDocCollector topDocsCollector = TopScoreDocCollector.create(10, true); 
    
    // docids collector for guiding facets accumulation (scoring disabled)
    ScoredDocIdCollector docIdsCollecor = ScoredDocIdCollector.create(indexReader.maxDoc(), false);
    
    // Faceted search parameters indicate which facets are we interested in 
    FacetSearchParams facetSearchParams = new FacetSearchParams(
        new CountFacetRequest(new CategoryPath("root", "a"), 10));
    
    // search, into both collectors. note: in case only facets accumulation 
    // is required, the topDocCollector part can be totally discarded
    searcher.search(q, MultiCollector.wrap(topDocsCollector, docIdsCollecor));
        
    // Obtain facets results and print them
    AdaptiveFacetsAccumulator accumulator = new AdaptiveFacetsAccumulator(facetSearchParams, indexReader, taxo);
    List<FacetResult> res = accumulator.accumulate(docIdsCollecor.getScoredDocIDs());
    
    int i = 0;
    for (FacetResult facetResult : res) {
      ExampleUtils.log("Res "+(i++)+": "+facetResult);
    }
    
    // we're done, close the index reader and the taxonomy.
    indexReader.close();
    taxo.close();
    
    return res;
  }
  
}
