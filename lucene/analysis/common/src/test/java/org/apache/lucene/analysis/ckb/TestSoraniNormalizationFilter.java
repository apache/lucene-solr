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
package org.apache.lucene.analysis.ckb;


import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

/**
 * Tests normalization for Sorani (this is more critical than stemming...)
 */
public class TestSoraniNormalizationFilter extends BaseTokenStreamTestCase {
  Analyzer a;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.KEYWORD, false);
        return new TokenStreamComponents(tokenizer, new SoraniNormalizationFilter(tokenizer));
      }
    };
  }
  
  @Override
  public void tearDown() throws Exception {
    a.close();
    super.tearDown();
  }
  
  public void testY() throws Exception {
    checkOneTerm(a, "\u064A", "\u06CC");
    checkOneTerm(a, "\u0649", "\u06CC");
    checkOneTerm(a, "\u06CC", "\u06CC");
  }
  
  public void testK() throws Exception {
    checkOneTerm(a, "\u0643", "\u06A9");
    checkOneTerm(a, "\u06A9", "\u06A9");
  }
  
  public void testH() throws Exception {
    // initial
    checkOneTerm(a, "\u0647\u200C", "\u06D5");
    // medial
    checkOneTerm(a, "\u0647\u200C\u06A9", "\u06D5\u06A9");
    
    checkOneTerm(a, "\u06BE", "\u0647");
    checkOneTerm(a, "\u0629", "\u06D5");
  }
  
  public void testFinalH() throws Exception {
    // always (and in final form by def), so frequently omitted
    checkOneTerm(a, "\u0647\u0647\u0647", "\u0647\u0647\u06D5");
  }
  
  public void testRR() throws Exception {
    checkOneTerm(a, "\u0692", "\u0695");
  }
  
  public void testInitialRR() throws Exception {
    // always, so frequently omitted
    checkOneTerm(a, "\u0631\u0631\u0631", "\u0695\u0631\u0631");
  }
  
  public void testRemove() throws Exception {
    checkOneTerm(a, "\u0640", "");
    checkOneTerm(a, "\u064B", "");
    checkOneTerm(a, "\u064C", "");
    checkOneTerm(a, "\u064D", "");
    checkOneTerm(a, "\u064E", "");
    checkOneTerm(a, "\u064F", "");
    checkOneTerm(a, "\u0650", "");
    checkOneTerm(a, "\u0651", "");
    checkOneTerm(a, "\u0652", "");
    // we peek backwards in this case to look for h+200C, ensure this works
    checkOneTerm(a, "\u200C", "");
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new SoraniNormalizationFilter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
}
