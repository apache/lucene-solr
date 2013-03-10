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
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.junit.BeforeClass;

/**
 * Tests {@link SweetSpotSimilarityFactory}
 */
public class TestSweetSpotSimilarityFactory extends BaseSimilarityTestCase {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml","schema-sweetspot.xml");
  }
  
  /** default parameters */
  public void testDefaults() throws Exception {
    SweetSpotSimilarity sim = getSimilarity("text", SweetSpotSimilarity.class);

    // SSS tf w/defaults should behave just like DS
    DefaultSimilarity d = new DefaultSimilarity();
    for (int i = 0; i <=1000; i++) {
      assertEquals("tf: i="+i, d.tf(i), sim.tf(i), 0.0F);
    }

    // default norm sanity check
    assertEquals("norm 1",  1.00F, sim.computeLengthNorm(1),  0.0F);
    assertEquals("norm 4",  0.50F, sim.computeLengthNorm(4),  0.0F);
    assertEquals("norm 16", 0.25F, sim.computeLengthNorm(16), 0.0F);
  }
  
  /** baseline with parameters */
  public void testBaselineParameters() throws Exception {
    SweetSpotSimilarity sim = getSimilarity("text_baseline", 
                                            SweetSpotSimilarity.class);
    
    DefaultSimilarity d = new DefaultSimilarity();

    // constant up to 6
    for (int i = 1; i <=6; i++) {
      assertEquals("tf i="+i, 1.5F, sim.tf(i), 0.0F);
    }
    // less then default sim above 6
    for (int i = 6; i <=1000; i++) {
      assertTrue("tf: i="+i+" : s="+sim.tf(i)+
                 " < d="+d.tf(i),
                 sim.tf(i) < d.tf(i));
    }

    // norms: plateau from 3-5
    assertEquals("norm 1 == 7", 
                 sim.computeLengthNorm(1), sim.computeLengthNorm(7),  0.0F);
    assertEquals("norm 2 == 6",  
                 sim.computeLengthNorm(1), sim.computeLengthNorm(7),  0.0F);
    assertEquals("norm 3",  1.00F, sim.computeLengthNorm(3),  0.0F);
    assertEquals("norm 4",  1.00F, sim.computeLengthNorm(4),  0.0F);
    assertEquals("norm 5",  1.00F, sim.computeLengthNorm(5),  0.0F);
    assertTrue("norm 6 too high: " + sim.computeLengthNorm(6),
               sim.computeLengthNorm(6) < 1.0F);
    assertTrue("norm 7 higher then norm 6", 
               sim.computeLengthNorm(7) < sim.computeLengthNorm(6));
    assertEquals("norm 20", 0.25F, sim.computeLengthNorm(20), 0.0F);
  }

  /** hyperbolic with parameters */
  public void testHyperbolicParameters() throws Exception {
    SweetSpotSimilarity sim = getSimilarity("text_hyperbolic", 
                                            SweetSpotSimilarity.class);

    for (int i = 1; i <=1000; i++) {
      assertTrue("MIN tf: i="+i+" : s="+sim.tf(i),
                 3.3F <= sim.tf(i));
      assertTrue("MAX tf: i="+i+" : s="+sim.tf(i),
                 sim.tf(i) <= 7.7F);
    }
    assertEquals("MID tf", 3.3F+(7.7F - 3.3F)/2.0F, sim.tf(5), 0.00001F);

    // norms: plateau from 1-5, shallow slope
    assertEquals("norm 1",  1.00F, sim.computeLengthNorm(1),  0.0F);
    assertEquals("norm 2",  1.00F, sim.computeLengthNorm(2),  0.0F);
    assertEquals("norm 3",  1.00F, sim.computeLengthNorm(3),  0.0F);
    assertEquals("norm 4",  1.00F, sim.computeLengthNorm(4),  0.0F);
    assertEquals("norm 5",  1.00F, sim.computeLengthNorm(5),  0.0F);
    assertTrue("norm 6 too high: " + sim.computeLengthNorm(6),
               sim.computeLengthNorm(6) < 1.0F);
    assertTrue("norm 7 higher then norm 6", 
               sim.computeLengthNorm(7) < sim.computeLengthNorm(6));
    assertTrue("norm 20 not high enough: " + sim.computeLengthNorm(20),
               0.25F < sim.computeLengthNorm(20));
  }
}
