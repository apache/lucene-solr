package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.util._TestUtil;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.facet.FacetTestBase;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;

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

public abstract class BaseTestTopK extends FacetTestBase {

  protected static final String ALPHA = "alpha";
  protected static final String BETA  = "beta";

  /** partition sizes on which the tests are run */
  protected static int[] partitionSizes = new int[] { 2, 3, 100, Integer.MAX_VALUE };

  /** Categories are generated from range [0,maxCategory) */
  protected static int maxCategory = 5000;
  private static final int categoriesPow2 = maxCategory * maxCategory;

  private int currDoc;
  private int nextInt;

  @Override
  protected void populateIndex(RandomIndexWriter iw, TaxonomyWriter taxo, FacetIndexingParams fip) throws IOException {
    currDoc = -1;
    super.populateIndex(iw, taxo, fip);
  }
  
  /** prepare the next random int */
  private void nextInt(int doc) {
    if (currDoc == doc ) {
      return;
    }
    currDoc = doc;
    // the code below tries to achieve non-uniform distribution of
    // categories. Perhaps we can use random.nextGaussian() instead,
    // something like nextGaussian() * stdev + maxCategory/2. Or
    // try to generate a Zipf distribution.
    nextInt = random().nextInt(categoriesPow2);
    nextInt = (int)Math.sqrt(nextInt);
  }
  
  @Override
  protected String getContent(int doc) {
    nextInt(doc);
    if (random().nextDouble() > 0.1) {
      return ALPHA + ' ' + BETA;
    }
    return ALPHA;
  }
  
  @Override
  protected List<CategoryPath> getCategories(int doc) {
    nextInt(doc);
    CategoryPath cp = new CategoryPath(
        "a", 
        Integer.toString(nextInt / 1000), 
        Integer.toString(nextInt / 100), 
        Integer.toString(nextInt / 10));
    if (VERBOSE) {
      System.out.println("Adding CP: " + cp.toString());
    }
    return Arrays.asList(cp);
  }

  protected FacetSearchParams searchParamsWithRequests(int numResults, FacetIndexingParams fip) {
    List<FacetRequest> facetRequests = new ArrayList<FacetRequest>();
    facetRequests.add(new CountFacetRequest(new CategoryPath("a"), numResults));
    facetRequests.add(new CountFacetRequest(new CategoryPath("a", "1"), numResults));
    facetRequests.add(new CountFacetRequest(new CategoryPath("a", "1", "10"), numResults));
    facetRequests.add(new CountFacetRequest(new CategoryPath("a", "2",  "26", "267"), numResults));
    return getFacetSearchParams(facetRequests, fip);
  }

  @Override
  protected int numDocsToIndex() {
    return 20000;
  }

  @Override
  protected IndexWriterConfig getIndexWriterConfig(Analyzer analyzer) {
    return super.getIndexWriterConfig(analyzer).setMaxBufferedDocs(_TestUtil.nextInt(random(), 500, 10000));
  }
}
