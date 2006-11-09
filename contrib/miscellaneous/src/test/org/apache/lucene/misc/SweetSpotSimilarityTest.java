
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

package org.apache.lucene.misc;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.File;
import java.math.BigDecimal;
import java.util.Random;
import java.util.Date;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Test of the SweetSpotSimilarity
 */
public class SweetSpotSimilarityTest extends TestCase {

  public void testSweetSpotLengthNorm() {
  
    SweetSpotSimilarity ss = new SweetSpotSimilarity();
    ss.setLengthNormFactors(1,1,0.5f);

    Similarity d = new DefaultSimilarity();
    Similarity s = ss;


    // base case, should degrade
  
    for (int i = 1; i < 1000; i++) {
      assertEquals("base case: i="+i,
                   d.lengthNorm("foo",i), s.lengthNorm("foo",i),
                   0.0f);
    }

    // make a sweet spot
  
    ss.setLengthNormFactors(3,10,0.5f);
  
    for (int i = 3; i <=10; i++) {
      assertEquals("3,10: spot i="+i,
                   1.0f, s.lengthNorm("foo",i),
                   0.0f);
    }
  
    for (int i = 10; i < 1000; i++) {
      assertEquals("3,10: 10<x : i="+i,
                   d.lengthNorm("foo",i-9), s.lengthNorm("foo",i),
                   0.0f);
    }


    // seperate sweet spot for certain fields

    ss.setLengthNormFactors("bar",8,13, 0.5f);
    ss.setLengthNormFactors("yak",6,9, 0.5f);

  
    for (int i = 3; i <=10; i++) {
      assertEquals("f: 3,10: spot i="+i,
                   1.0f, s.lengthNorm("foo",i),
                   0.0f);
    }
    for (int i = 10; i < 1000; i++) {
      assertEquals("f: 3,10: 10<x : i="+i,
                   d.lengthNorm("foo",i-9), s.lengthNorm("foo",i),
                   0.0f);
    }
    for (int i = 8; i <=13; i++) {
      assertEquals("f: 8,13: spot i="+i,
                   1.0f, s.lengthNorm("bar",i),
                   0.0f);
    }
    for (int i = 6; i <=9; i++) {
      assertEquals("f: 6,9: spot i="+i,
                   1.0f, s.lengthNorm("yak",i),
                   0.0f);
    }
    for (int i = 13; i < 1000; i++) {
      assertEquals("f: 8,13: 13<x : i="+i,
                   d.lengthNorm("foo",i-12), s.lengthNorm("bar",i),
                   0.0f);
    }
    for (int i = 9; i < 1000; i++) {
      assertEquals("f: 6,9: 9<x : i="+i,
                   d.lengthNorm("foo",i-8), s.lengthNorm("yak",i),
                   0.0f);
    }


    // steepness

    ss.setLengthNormFactors("a",5,8,0.5f);
    ss.setLengthNormFactors("b",5,8,0.1f);

    for (int i = 9; i < 1000; i++) {
      assertTrue("s: i="+i+" : a="+ss.lengthNorm("a",i)+
                 " < b="+ss.lengthNorm("b",i),
                 ss.lengthNorm("a",i) < s.lengthNorm("b",i));
    }

  }

  public void testSweetSpotTf() {
  
    SweetSpotSimilarity ss = new SweetSpotSimilarity();

    Similarity d = new DefaultSimilarity();
    Similarity s = ss;
    
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
        public float tf(int freq) {
          return hyperbolicTf(freq);
        }
      };
    ss.setHyperbolicTfFactors(3.3f, 7.7f, Math.E, 5.0f);
    
    Similarity s = ss;

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

