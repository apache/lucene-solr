package org.apache.lucene.facet.example.association;

import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;

import org.apache.lucene.facet.example.simple.SimpleSearcher;
import org.apache.lucene.facet.search.params.association.AssociationFloatSumFacetRequest;
import org.apache.lucene.facet.search.params.association.AssociationIntSumFacetRequest;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.lucene.LuceneTaxonomyReader;

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

/**
 * AssociationSearcher searches index with facets, evaluating the facets with
 * their associated $int value
 * 
 * @lucene.experimental
 */
public class AssociationSearcher {

  /** Search an index with a sum of int-association. */
  public static List<FacetResult> searchSumIntAssociation(Directory indexDir,
      Directory taxoDir) throws Exception {
    // prepare index reader 
    IndexReader indexReader = IndexReader.open(indexDir);
    TaxonomyReader taxo = new LuceneTaxonomyReader(taxoDir);
    
    AssociationIntSumFacetRequest facetRequest = new AssociationIntSumFacetRequest(
        new CategoryPath("tags"), 10);
    
    List<FacetResult> res = SimpleSearcher.searchWithRequest(indexReader, taxo,
        AssociationUtils.assocIndexingParams, facetRequest);
    
    // close readers
    taxo.close();
    indexReader.close();
    
    return res;
  }
  
  /** Search an index with a sum of float-association. */
  public static List<FacetResult> searchSumFloatAssociation(Directory indexDir,
      Directory taxoDir) throws Exception {
    // prepare index reader 
    IndexReader indexReader = IndexReader.open(indexDir);
    TaxonomyReader taxo = new LuceneTaxonomyReader(taxoDir);
    
    AssociationFloatSumFacetRequest facetRequest = new AssociationFloatSumFacetRequest(
        new CategoryPath("genre"), 10);
    
    List<FacetResult> res = SimpleSearcher.searchWithRequest(indexReader, taxo,
        AssociationUtils.assocIndexingParams, facetRequest);
    
    // close readers
    taxo.close();
    indexReader.close();
    
    return res;
  }
  
}
