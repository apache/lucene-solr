package org.apache.solr.schema;

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

import org.apache.lucene.misc.SweetSpotSimilarity;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Similarity;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.junit.BeforeClass;

/**
 * Tests per-field similarity support in the schema
 */
public class TestPerFieldSimilarity extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }
  
  /** test a field where the sim is specified directly */
  public void testDirect() throws Exception {
    SolrCore core = h.getCore();
    RefCounted<SolrIndexSearcher> searcher = core.getSearcher();
    Similarity sim = searcher.get().getSimilarityProvider().get("sim1text");
    assertEquals(SweetSpotSimilarity.class, sim.getClass());
    searcher.decref();
  }
  
  /** ... and for a dynamic field */
  public void testDirectDynamic() throws Exception {
    SolrCore core = h.getCore();
    RefCounted<SolrIndexSearcher> searcher = core.getSearcher();
    Similarity sim = searcher.get().getSimilarityProvider().get("text_sim1");
    assertEquals(SweetSpotSimilarity.class, sim.getClass());
    searcher.decref();
  }
  
  /** test a field where a configurable sim factory is defined */
  public void testFactory() throws Exception {
    SolrCore core = h.getCore();
    RefCounted<SolrIndexSearcher> searcher = core.getSearcher();
    Similarity sim = searcher.get().getSimilarityProvider().get("sim2text");
    assertEquals(MockConfigurableSimilarity.class, sim.getClass());
    assertEquals("is there an echo?", ((MockConfigurableSimilarity)sim).getPassthrough());
    searcher.decref();
  }
  
  /** ... and for a dynamic field */
  public void testFactoryDynamic() throws Exception {
    SolrCore core = h.getCore();
    RefCounted<SolrIndexSearcher> searcher = core.getSearcher();
    Similarity sim = searcher.get().getSimilarityProvider().get("text_sim2");
    assertEquals(MockConfigurableSimilarity.class, sim.getClass());
    assertEquals("is there an echo?", ((MockConfigurableSimilarity)sim).getPassthrough());
    searcher.decref();
  }
  
  /** test a field where no similarity is specified */
  public void testDefaults() throws Exception {
    SolrCore core = h.getCore();
    RefCounted<SolrIndexSearcher> searcher = core.getSearcher();
    Similarity sim = searcher.get().getSimilarityProvider().get("sim3text");
    assertEquals(MockConfigurableSimilarity.class, sim.getClass());
    assertEquals("I am your default sim", ((MockConfigurableSimilarity)sim).getPassthrough());
    searcher.decref();
  }
  
  /** ... and for a dynamic field */
  public void testDefaultsDynamic() throws Exception {
    SolrCore core = h.getCore();
    RefCounted<SolrIndexSearcher> searcher = core.getSearcher();
    Similarity sim = searcher.get().getSimilarityProvider().get("text_sim3");
    assertEquals(MockConfigurableSimilarity.class, sim.getClass());
    assertEquals("I am your default sim", ((MockConfigurableSimilarity)sim).getPassthrough());
    searcher.decref();
  }
  
  /** test a field that does not exist */
  public void testNonexistent() throws Exception {
    SolrCore core = h.getCore();
    RefCounted<SolrIndexSearcher> searcher = core.getSearcher();
    Similarity sim = searcher.get().getSimilarityProvider().get("sdfdsfdsfdswr5fsdfdsfdsfs");
    assertEquals(MockConfigurableSimilarity.class, sim.getClass());
    assertEquals("I am your default sim", ((MockConfigurableSimilarity)sim).getPassthrough());
    searcher.decref();
  }
}
