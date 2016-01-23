package org.apache.solr.search.similarities;

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

import org.apache.lucene.misc.SweetSpotSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Version;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Tests per-field similarity support in the schema when luceneMatchVersion indicates 
 * {@link ClassicSimilarity} should be the default.
 * @see TestPerFieldSimilarity
 */
public class TestPerFieldSimilarityClassic extends BaseSimilarityTestCase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    // any value below 6.0 should have this behavior
    System.setProperty("tests.luceneMatchVersion", Version.LUCENE_5_3_1.toString());
    initCore("solrconfig-basic.xml","schema-sim.xml");
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    System.clearProperty("tests.luceneMatchVersion");
  }

  /** test a field where the sim is specified directly */
  public void testDirect() throws Exception {
    assertEquals(SweetSpotSimilarity.class, getSimilarity("sim1text").getClass());
  }
  
  /** ... and for a dynamic field */
  public void testDirectDynamic() throws Exception {
    assertEquals(SweetSpotSimilarity.class, getSimilarity("text_sim1").getClass());
  }
  
  /** test a field where a configurable sim factory is defined */
  public void testFactory() throws Exception {
    Similarity sim = getSimilarity("sim2text");
    assertEquals(MockConfigurableSimilarity.class, sim.getClass());
    assertEquals("is there an echo?", ((MockConfigurableSimilarity)sim).getPassthrough());
  }
  
  /** ... and for a dynamic field */
  public void testFactoryDynamic() throws Exception {
    Similarity sim = getSimilarity("text_sim2");
    assertEquals(MockConfigurableSimilarity.class, sim.getClass());
    assertEquals("is there an echo?", ((MockConfigurableSimilarity)sim).getPassthrough());
  }
  
  /** test a field where no similarity is specified */
  public void testDefaults() throws Exception {
    Similarity sim = getSimilarity("sim3text");
    assertEquals(ClassicSimilarity.class, sim.getClass());;
  }
  
  /** ... and for a dynamic field */
  public void testDefaultsDynamic() throws Exception {
    Similarity sim = getSimilarity("text_sim3");
    assertEquals(ClassicSimilarity.class, sim.getClass());
  }
  
  /** test a field that does not exist */
  public void testNonexistent() throws Exception {
    Similarity sim = getSimilarity("sdfdsfdsfdswr5fsdfdsfdsfs");
    assertEquals(ClassicSimilarity.class, sim.getClass());
  }
}
