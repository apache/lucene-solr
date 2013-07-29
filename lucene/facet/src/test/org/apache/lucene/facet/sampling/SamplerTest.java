package org.apache.lucene.facet.sampling;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.facet.FacetTestBase;
import org.apache.lucene.facet.old.OldFacetsAccumulator;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.CountFacetRequest;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.junit.After;
import org.junit.Before;

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

public class SamplerTest extends FacetTestBase {
  
  private FacetIndexingParams fip;
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    fip = getFacetIndexingParams(Integer.MAX_VALUE);
    initIndex(fip);
  }
  
  @Override
  protected int numDocsToIndex() {
    return 100;
  }
  
  @Override
  protected List<CategoryPath> getCategories(final int doc) {
    return new ArrayList<CategoryPath>() {
      {
        add(new CategoryPath("root", "a", Integer.toString(doc % 10)));
      }
    };
  }
  
  @Override
  protected String getContent(int doc) {
    return "";
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    closeAll();
    super.tearDown();
  }
  
  public void testDefaultFixer() throws Exception {
    RandomSampler randomSampler = new RandomSampler();
    SampleFixer fixer = randomSampler.samplingParams.getSampleFixer();
    assertEquals(null, fixer);
  }
  
  public void testCustomFixer() throws Exception {
    SamplingParams sp = new SamplingParams();
    sp.setSampleFixer(new TakmiSampleFixer(null, null, null));
    assertEquals(TakmiSampleFixer.class, sp.getSampleFixer().getClass());
  }
  
  public void testNoFixing() throws Exception {
    SamplingParams sp = new SamplingParams();
    sp.setMaxSampleSize(10);
    sp.setMinSampleSize(5);
    sp.setSampleRatio(0.01d);
    sp.setSamplingThreshold(50);
    sp.setOversampleFactor(5d);
    
    assertNull("Fixer should be null as the test is for no-fixing",
        sp.getSampleFixer());
    FacetSearchParams fsp = new FacetSearchParams(fip, new CountFacetRequest(
        new CategoryPath("root", "a"), 1));
    SamplingAccumulator accumulator = new SamplingAccumulator(
        new RandomSampler(sp, random()), fsp, indexReader, taxoReader);
    
    // Make sure no complements are in action
    accumulator
        .setComplementThreshold(OldFacetsAccumulator.DISABLE_COMPLEMENT);
    
    FacetsCollector fc = FacetsCollector.create(accumulator);
    
    searcher.search(new MatchAllDocsQuery(), fc);
    FacetResultNode node = fc.getFacetResults().get(0).getFacetResultNode();
    
    assertTrue(node.value < numDocsToIndex());
  }
}
