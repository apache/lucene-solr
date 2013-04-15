package org.apache.lucene.facet.sampling;

import java.io.IOException;
import java.util.Collections;

import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.sampling.RandomSampler;
import org.apache.lucene.facet.sampling.Sampler;
import org.apache.lucene.facet.sampling.SamplingAccumulator;
import org.apache.lucene.facet.sampling.SamplingParams;
import org.apache.lucene.facet.search.CountFacetRequest;
import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.search.StandardFacetsAccumulator;
import org.apache.lucene.facet.search.FacetRequest.ResultMode;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.junit.Test;

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

public class OversampleWithDepthTest extends FacetTestCase {
  
  @Test
  public void testCountWithdepthUsingSampling() throws Exception, IOException {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    
    FacetIndexingParams fip = new FacetIndexingParams(randomCategoryListParams());
    
    // index 100 docs, each with one category: ["root", docnum/10, docnum]
    // e.g. root/8/87
    index100Docs(indexDir, taxoDir, fip);
    
    DirectoryReader r = DirectoryReader.open(indexDir);
    TaxonomyReader tr = new DirectoryTaxonomyReader(taxoDir);
    
    CountFacetRequest facetRequest = new CountFacetRequest(new CategoryPath("root"), 10);
    // Setting the depth to '2', should potentially get all categories
    facetRequest.setDepth(2);
    facetRequest.setResultMode(ResultMode.PER_NODE_IN_TREE);

    FacetSearchParams fsp = new FacetSearchParams(fip, facetRequest);
    
    // Craft sampling params to enforce sampling
    final SamplingParams params = new SamplingParams();
    params.setMinSampleSize(2);
    params.setMaxSampleSize(50);
    params.setOversampleFactor(5);
    params.setSamplingThreshold(60);
    params.setSampleRatio(0.1);
    
    FacetResult res = searchWithFacets(r, tr, fsp, params);
    FacetRequest req = res.getFacetRequest();
    assertEquals(facetRequest, req);
    
    FacetResultNode rootNode = res.getFacetResultNode();
    
    // Each node below root should also have sub-results as the requested depth was '2'
    for (FacetResultNode node : rootNode.subResults) {
      assertTrue("node " + node.label + " should have had children as the requested depth was '2'", node.subResults.size() > 0);
    }
    
    IOUtils.close(r, tr, indexDir, taxoDir);
  }

  private void index100Docs(Directory indexDir, Directory taxoDir, FacetIndexingParams fip) throws IOException {
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, null);
    IndexWriter w = new IndexWriter(indexDir, iwc);
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(taxoDir);
    
    FacetFields facetFields = new FacetFields(tw, fip);
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      CategoryPath cp = new CategoryPath("root",Integer.toString(i / 10), Integer.toString(i));
      facetFields.addFields(doc, Collections.singletonList(cp));
      w.addDocument(doc);
    }
    IOUtils.close(tw, w);
  }

  /** search reader <code>r</code>*/
  private FacetResult searchWithFacets(IndexReader r, TaxonomyReader tr, FacetSearchParams fsp, 
      final SamplingParams params) throws IOException {
    // a FacetsCollector with a sampling accumulator
    Sampler sampler = new RandomSampler(params, random());
    StandardFacetsAccumulator sfa = new SamplingAccumulator(sampler, fsp, r, tr);
    FacetsCollector fcWithSampling = FacetsCollector.create(sfa);
    
    IndexSearcher s = newSearcher(r);
    s.search(new MatchAllDocsQuery(), fcWithSampling);
    
    // there's only one expected result, return just it.
    return fcWithSampling.getFacetResults().get(0);
  }
}
