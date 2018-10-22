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

import java.io.IOException;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.misc.SweetSpotSimilarity;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.junit.BeforeClass;

/**
 * Tests {@link SweetSpotSimilarityFactory}
 */
public class TestSweetSpotSimilarityFactory extends BaseSimilarityTestCase {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml","schema-sweetspot.xml");
  }

  private static float computeNorm(Similarity sim, int length) throws IOException {
    String value = IntStream.range(0, length).mapToObj(i -> "a").collect(Collectors.joining(" "));
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setSimilarity(sim));
    w.addDocument(Collections.singleton(newTextField("foo", value, Store.NO)));
    DirectoryReader reader = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setSimilarity(sim);
    Explanation expl = searcher.explain(new TermQuery(new Term("foo", "a")), 0);
    reader.close();
    dir.close();
    Explanation norm = findExplanation(expl, "fieldNorm");
    assertNotNull(norm);
    return norm.getValue().floatValue();
  }

  private static Explanation findExplanation(Explanation expl, String text) {
    if (expl.getDescription().startsWith(text)) {
      return expl;
    } else {
      for (Explanation sub : expl.getDetails()) {
        Explanation match = findExplanation(sub, text);
        if (match != null) {
          return match;
        }
      }
    }
    return null;
  }

  /** default parameters */
  public void testDefaults() throws Exception {
    SweetSpotSimilarity sim = getSimilarity("text", SweetSpotSimilarity.class);

    // SSS tf w/defaults should behave just like DS
    ClassicSimilarity d = new ClassicSimilarity();
    for (int i = 0; i <=1000; i++) {
      assertEquals("tf: i="+i, d.tf(i), sim.tf(i), 0.0F);
    }

    // default norm sanity check
    assertEquals("norm 1",  1.00F, computeNorm(sim, 1),  0.0F);
    assertEquals("norm 4",  0.50F, computeNorm(sim, 4),  0.0F);
    assertEquals("norm 16", 0.25F, computeNorm(sim, 16), 0.0F);
  }
  
  /** baseline with parameters */
  public void testBaselineParameters() throws Exception {
    SweetSpotSimilarity sim = getSimilarity("text_baseline", 
                                            SweetSpotSimilarity.class);
    
    ClassicSimilarity d = new ClassicSimilarity();

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
                 computeNorm(sim, 1), computeNorm(sim, 7),  0.0F);
    assertEquals("norm 2 == 6",  
                 computeNorm(sim, 1), computeNorm(sim, 7),  0.0F);
    assertEquals("norm 3",  1.00F, computeNorm(sim, 3),  0.0F);
    assertEquals("norm 4",  1.00F, computeNorm(sim, 4),  0.0F);
    assertEquals("norm 5",  1.00F, computeNorm(sim, 5),  0.0F);
    assertTrue("norm 6 too high: " + computeNorm(sim, 6),
               computeNorm(sim, 6) < 1.0F);
    assertTrue("norm 7 higher then norm 6", 
               computeNorm(sim, 7) < computeNorm(sim, 6));
    assertEquals("norm 20", 0.25F, computeNorm(sim, 20), 0.0F);
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
    assertEquals("norm 1",  1.00F, computeNorm(sim, 1),  0.0F);
    assertEquals("norm 2",  1.00F, computeNorm(sim, 2),  0.0F);
    assertEquals("norm 3",  1.00F, computeNorm(sim, 3),  0.0F);
    assertEquals("norm 4",  1.00F, computeNorm(sim, 4),  0.0F);
    assertEquals("norm 5",  1.00F, computeNorm(sim, 5),  0.0F);
    assertTrue("norm 6 too high: " + computeNorm(sim, 6),
               computeNorm(sim, 6) < 1.0F);
    assertTrue("norm 7 higher then norm 6", 
               computeNorm(sim, 7) < computeNorm(sim, 6));
    assertTrue("norm 20 not high enough: " + computeNorm(sim, 20),
               0.25F < computeNorm(sim, 20));
  }
}
