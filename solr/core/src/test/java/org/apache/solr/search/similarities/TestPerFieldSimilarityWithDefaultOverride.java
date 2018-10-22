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
package org.apache.solr.search.similarities;

import org.apache.lucene.misc.SweetSpotSimilarity;
import org.junit.BeforeClass;

/**
 * Tests per-field similarity support in the schema when SchemaSimilarityFactory is explicitly
 * configured to use a custom default sim for field types that do not override it.
 * @see TestPerFieldSimilarity
 */
public class TestPerFieldSimilarityWithDefaultOverride extends BaseSimilarityTestCase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml","schema-sim-default-override.xml");
  }
  
  /** test a field where the sim is specified directly */
  public void testDirect() throws Exception {
    assertNotNull(getSimilarity("sim1text", SweetSpotSimilarity.class));
  }
  
  /** ... and for a dynamic field */
  public void testDirectDynamic() throws Exception {
    assertNotNull(getSimilarity("text_sim1", SweetSpotSimilarity.class));
  }
  
  /** test a field where a configurable sim factory is explicitly defined */
  public void testDirectFactory() throws Exception {
    MockConfigurableSimilarity sim = getSimilarity("sim2text", MockConfigurableSimilarity.class);
    assertEquals("is there an echo?", sim.getPassthrough());
  }
  
  /** ... and for a dynamic field */
  public void testDirectFactoryDynamic() throws Exception {
    MockConfigurableSimilarity sim = getSimilarity("text_sim2", MockConfigurableSimilarity.class);
    assertEquals("is there an echo?", sim.getPassthrough());
  }
  
  /** test a field where no similarity is specified */
  public void testDefaults() throws Exception {
    MockConfigurableSimilarity sim = getSimilarity("sim3text", MockConfigurableSimilarity.class);
    assertEquals("is there an echo?", sim.getPassthrough());
  }
  
  /** ... and for a dynamic field */
  public void testDefaultsDynamic() throws Exception {
    MockConfigurableSimilarity sim = getSimilarity("text_sim3", MockConfigurableSimilarity.class);
    assertEquals("is there an echo?", sim.getPassthrough());
  }
  
  /** test a field that does not exist */
  public void testNonexistent() throws Exception {
    MockConfigurableSimilarity sim = getSimilarity("text_sim3", MockConfigurableSimilarity.class);
    assertEquals("is there an echo?", sim.getPassthrough());
  }
}
