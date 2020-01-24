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
package org.apache.lucene.misc;

import java.io.IOException;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Test of the SweetSpotSimilarity
 */
public class SweetSpotSimilarityTest extends LuceneTestCase {
  
  private static float computeNorm(Similarity sim, String field, int length) throws IOException {
    String value = IntStream.range(0, length).mapToObj(i -> "a").collect(Collectors.joining(" "));
    Directory dir = new RAMDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setSimilarity(sim));
    w.addDocument(Collections.singleton(newTextField(field, value, Store.NO)));
    DirectoryReader reader = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setSimilarity(sim);
    Explanation expl = searcher.explain(new TermQuery(new Term(field, "a")), 0);
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

  // TODO: rewrite this test to not make thosuands of indexes.
  @Nightly
  public void testSweetSpotComputeNorm() throws IOException {
  
    final SweetSpotSimilarity ss = new SweetSpotSimilarity();
    ss.setLengthNormFactors(1,1,0.5f,true);

    Similarity d = new ClassicSimilarity();
    Similarity s = ss;


    // base case, should degrade
    for (int i = 1; i < 1000; i++) {
      assertEquals("base case: i="+i,
                   computeNorm(d, "bogus", i),
                   computeNorm(s, "bogus", i),
                   0.0f);
    }

    // make a sweet spot
  
    ss.setLengthNormFactors(3,10,0.5f,true);
  
    for (int i = 3; i <=10; i++) {
      assertEquals("3,10: spot i="+i,
                   1.0f,
                   computeNorm(ss, "bogus", i),
                   0.0f);
    }
  
    for (int i = 10; i < 1000; i++) {
      final float normD = computeNorm(d, "bogus", i - 9);
      final float normS = computeNorm(s, "bogus", i);
      assertEquals("3,10: 10<x : i="+i,
                   normD,
                   normS,
                   0.01f);
    }


    // separate sweet spot for certain fields

    final SweetSpotSimilarity ssBar = new SweetSpotSimilarity();
    ssBar.setLengthNormFactors(8,13, 0.5f, false);
    final SweetSpotSimilarity ssYak = new SweetSpotSimilarity();
    ssYak.setLengthNormFactors(6,9, 0.5f, false);
    final SweetSpotSimilarity ssA = new SweetSpotSimilarity();
    ssA.setLengthNormFactors(5,8,0.5f, false);
    final SweetSpotSimilarity ssB = new SweetSpotSimilarity();
    ssB.setLengthNormFactors(5,8,0.1f, false);
    
    Similarity sp = new PerFieldSimilarityWrapper() {
      @Override
      public Similarity get(String field) {
        if (field.equals("bar"))
          return ssBar;
        else if (field.equals("yak"))
          return ssYak;
        else if (field.equals("a"))
          return ssA;
        else if (field.equals("b"))
          return ssB;
        else
          return ss;
      }
    };

    for (int i = 3; i <=10; i++) {
      assertEquals("f: 3,10: spot i="+i,
                   1.0f,
                   computeNorm(sp, "foo", i),
                   0.0f);
    }
    
    for (int i = 10; i < 1000; i++) {
      final float normD = computeNorm(d, "foo", i-9);
      final float normS = computeNorm(sp, "foo", i);
      assertEquals("f: 3,10: 10<x : i="+i,
                   normD,
                   normS,
                   0.01f);
    }
    
    for (int i = 8; i <=13; i++) {
      assertEquals("f: 8,13: spot i="+i,
                   1.0f,
                   computeNorm(sp, "bar", i),
                   0.01f);
    }
    
    for (int i = 6; i <=9; i++) {
      assertEquals("f: 6,9: spot i="+i,
                   1.0f,
                   computeNorm(sp, "yak", i),
                   0.01f);
    }
    
    for (int i = 13; i < 1000; i++) {
      final float normD = computeNorm(d, "bar", i-12);
      final float normS = computeNorm(sp, "bar", i);
      assertEquals("f: 8,13: 13<x : i="+i,
                   normD,
                   normS,
                   0.01f);
    }
    
    for (int i = 9; i < 1000; i++) {
      final float normD = computeNorm(d, "yak", i-8);
      final float normS = computeNorm(sp, "yak", i);
      assertEquals("f: 6,9: 9<x : i="+i,
                   normD,
                   normS,
                   0.01f);
    }


    // steepness

    for (int i = 9; i < 1000; i++) {
      final float normSS = computeNorm(sp, "a", i);
      final float normS = computeNorm(sp, "b", i);
      assertTrue("s: i="+i+" : a="+normSS+
                 " < b="+normS,
                 normSS < normS);
    }

  }

  public void testSweetSpotTf() {
  
    SweetSpotSimilarity ss = new SweetSpotSimilarity();

    TFIDFSimilarity d = new ClassicSimilarity();
    TFIDFSimilarity s = ss;
    
    // tf equal

    ss.setBaselineTfFactors(0.0f, 0.0f);
  
    for (int i = 1; i < 1000; i++) {
      assertEquals("tf: i="+i,
                   d.tf(i), s.tf(i), 0.0f);
    }

    // tf higher
  
    ss.setBaselineTfFactors(1.0f, 0.0f);
  
    for (int i = 1; i < 1000; i++) {
      assertTrue("tf: i="+i+" : d="+d.tf(i)+
                 " < s="+s.tf(i),
                 d.tf(i) < s.tf(i));
    }

    // tf flat
  
    ss.setBaselineTfFactors(1.0f, 6.0f);
    for (int i = 1; i <=6; i++) {
      assertEquals("tf flat1: i="+i, 1.0f, s.tf(i), 0.0f);
    }
    ss.setBaselineTfFactors(2.0f, 6.0f);
    for (int i = 1; i <=6; i++) {
      assertEquals("tf flat2: i="+i, 2.0f, s.tf(i), 0.0f);
    }
    for (int i = 6; i <=1000; i++) {
      assertTrue("tf: i="+i+" : s="+s.tf(i)+
                 " < d="+d.tf(i),
                 s.tf(i) < d.tf(i));
    }

    // stupidity
    assertEquals("tf zero", 0.0f, s.tf(0), 0.0f);
  }

  public void testHyperbolicSweetSpot() {
  
    SweetSpotSimilarity ss = new SweetSpotSimilarity() {
        @Override
        public float tf(float freq) {
          return hyperbolicTf(freq);
        }
      };
    ss.setHyperbolicTfFactors(3.3f, 7.7f, Math.E, 5.0f);
    
    TFIDFSimilarity s = ss;

    for (int i = 1; i <=1000; i++) {
      assertTrue("MIN tf: i="+i+" : s="+s.tf(i),
                 3.3f <= s.tf(i));
      assertTrue("MAX tf: i="+i+" : s="+s.tf(i),
                 s.tf(i) <= 7.7f);
    }
    assertEquals("MID tf", 3.3f+(7.7f - 3.3f)/2.0f, s.tf(5), 0.00001f);
    
    // stupidity
    assertEquals("tf zero", 0.0f, s.tf(0), 0.0f);
    
  }

  
}

