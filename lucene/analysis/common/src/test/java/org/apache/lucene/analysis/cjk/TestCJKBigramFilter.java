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
package org.apache.lucene.analysis.cjk;


import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.IOUtils;

public class TestCJKBigramFilter extends BaseTokenStreamTestCase {
  Analyzer analyzer, unibiAnalyzer;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer t = new StandardTokenizer();
        return new TokenStreamComponents(t, new CJKBigramFilter(t));
      }
    };
    unibiAnalyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer t = new StandardTokenizer();
        return new TokenStreamComponents(t, 
            new CJKBigramFilter(t, 0xff, true));
      }
    };
  }
  
  @Override
  public void tearDown() throws Exception {
    IOUtils.close(analyzer, unibiAnalyzer);
    super.tearDown();
  }
  
  public void testHuge() throws Exception {
    assertAnalyzesTo(analyzer, "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた"
     + "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた"
     + "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた",
       new String[] { 
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた", "た多",
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた", "た多",
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた", "た多",
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた", "た多",
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた", "た多",
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた", "た多",
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた", "た多",
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた", "た多",
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた", "た多",
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた", "た多",
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた"
       }    
    );
  }
  
  public void testHanOnly() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer t = new StandardTokenizer();
        return new TokenStreamComponents(t, new CJKBigramFilter(t, CJKBigramFilter.HAN));
      }
    };
    assertAnalyzesTo(a, "多くの学生が試験に落ちた。",
        new String[] { "多", "く", "の",  "学生", "が",  "試験", "に",  "落", "ち", "た" },
        new int[] { 0, 1, 2, 3, 5, 6, 8, 9, 10, 11 },
        new int[] { 1, 2, 3, 5, 6, 8, 9, 10, 11, 12 },
        new String[] { "<SINGLE>", "<HIRAGANA>", "<HIRAGANA>", "<DOUBLE>", "<HIRAGANA>", "<DOUBLE>", 
                       "<HIRAGANA>", "<SINGLE>", "<HIRAGANA>", "<HIRAGANA>", "<SINGLE>" },
        new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },
        new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 });
    a.close();
  }
  
  public void testAllScripts() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer t = new StandardTokenizer();
        return new TokenStreamComponents(t, 
            new CJKBigramFilter(t, 0xff, false));
      }
    };
    assertAnalyzesTo(a, "多くの学生が試験に落ちた。",
        new String[] { "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた" });
    a.close();
  }
  
  public void testUnigramsAndBigramsAllScripts() throws Exception {
    assertAnalyzesTo(unibiAnalyzer, "多くの学生が試験に落ちた。",
        new String[] { 
        "多", "多く", "く",  "くの", "の",  "の学", "学", "学生", "生", 
        "生が", "が",  "が試", "試", "試験", "験", "験に", "に", 
                "に落", "落", "落ち", "ち", "ちた", "た" 
        },
        new int[] { 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6,
                    6, 7, 7, 8, 8, 9, 9, 10, 10, 11 },
        new int[] { 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 
                    8, 8, 9, 9, 10, 10, 11, 11, 12, 12 },
        new String[] { "<SINGLE>", "<DOUBLE>", "<SINGLE>", "<DOUBLE>", "<SINGLE>", "<DOUBLE>", "<SINGLE>", "<DOUBLE>",
                       "<SINGLE>", "<DOUBLE>", "<SINGLE>", "<DOUBLE>", "<SINGLE>", "<DOUBLE>", "<SINGLE>", "<DOUBLE>",
                       "<SINGLE>", "<DOUBLE>", "<SINGLE>", "<DOUBLE>", "<SINGLE>", "<DOUBLE>", "<SINGLE>" },
        new int[] { 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 
                    0, 1, 0, 1, 0, 1, 0, 1, 0, 1 },
        new int[] { 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 
                    2, 1, 2, 1, 2, 1, 2, 1, 2, 1 }
    );
  }
  
  public void testUnigramsAndBigramsHanOnly() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer t = new StandardTokenizer();
        return new TokenStreamComponents(t, new CJKBigramFilter(t, CJKBigramFilter.HAN, true));
      }
    };
    assertAnalyzesTo(a, "多くの学生が試験に落ちた。",
        new String[] { "多", "く", "の",  "学", "学生", "生", "が",  "試", "試験", "験", "に",  "落", "ち", "た" },
        new int[] { 0, 1, 2, 3, 3, 4, 5, 6, 6, 7, 8, 9, 10, 11 },
        new int[] { 1, 2, 3, 4, 5, 5, 6, 7, 8, 8, 9, 10, 11, 12 },
        new String[] { "<SINGLE>", "<HIRAGANA>", "<HIRAGANA>", "<SINGLE>", "<DOUBLE>", 
                       "<SINGLE>", "<HIRAGANA>", "<SINGLE>", "<DOUBLE>", "<SINGLE>", 
                       "<HIRAGANA>", "<SINGLE>", "<HIRAGANA>", "<HIRAGANA>", "<SINGLE>" },
        new int[] { 1, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 1, 1 },
        new int[] { 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 1 });
    a.close();
  }
  
  public void testUnigramsAndBigramsHuge() throws Exception {
    assertAnalyzesTo(unibiAnalyzer, "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた"
     + "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた"
     + "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた",
       new String[] { 
        "多", "多く", "く",  "くの", "の", "の学", "学", "学生", "生", "生が", "が",  "が試", "試", "試験", "験", "験に", "に",  "に落", "落", "落ち", "ち", "ちた", "た", "た多",
        "多", "多く", "く",  "くの", "の", "の学", "学", "学生", "生", "生が", "が",  "が試", "試", "試験", "験", "験に", "に",  "に落", "落", "落ち", "ち", "ちた", "た", "た多",
        "多", "多く", "く",  "くの", "の", "の学", "学", "学生", "生", "生が", "が",  "が試", "試", "試験", "験", "験に", "に",  "に落", "落", "落ち", "ち", "ちた", "た", "た多",
        "多", "多く", "く",  "くの", "の", "の学", "学", "学生", "生", "生が", "が",  "が試", "試", "試験", "験", "験に", "に",  "に落", "落", "落ち", "ち", "ちた", "た", "た多",
        "多", "多く", "く",  "くの", "の", "の学", "学", "学生", "生", "生が", "が",  "が試", "試", "試験", "験", "験に", "に",  "に落", "落", "落ち", "ち", "ちた", "た", "た多",
        "多", "多く", "く",  "くの", "の", "の学", "学", "学生", "生", "生が", "が",  "が試", "試", "試験", "験", "験に", "に",  "に落", "落", "落ち", "ち", "ちた", "た", "た多",
        "多", "多く", "く",  "くの", "の", "の学", "学", "学生", "生", "生が", "が",  "が試", "試", "試験", "験", "験に", "に",  "に落", "落", "落ち", "ち", "ちた", "た", "た多",
        "多", "多く", "く",  "くの", "の", "の学", "学", "学生", "生", "生が", "が",  "が試", "試", "試験", "験", "験に", "に",  "に落", "落", "落ち", "ち", "ちた", "た", "た多",
        "多", "多く", "く",  "くの", "の", "の学", "学", "学生", "生", "生が", "が",  "が試", "試", "試験", "験", "験に", "に",  "に落", "落", "落ち", "ち", "ちた", "た", "た多",
        "多", "多く", "く",  "くの", "の", "の学", "学", "学生", "生", "生が", "が",  "が試", "試", "試験", "験", "験に", "に",  "に落", "落", "落ち", "ち", "ちた", "た", "た多",
        "多", "多く", "く",  "くの", "の", "の学", "学", "学生", "生", "生が", "が",  "が試", "試", "試験", "験", "験に", "に",  "に落", "落", "落ち", "ち", "ちた", "た"
       }    
    );
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomUnibiStrings() throws Exception {
    checkRandomData(random(), unibiAnalyzer, 1000*RANDOM_MULTIPLIER);
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomUnibiHugeStrings() throws Exception {
    Random random = random();
    checkRandomData(random, unibiAnalyzer, 100*RANDOM_MULTIPLIER, 8192);
  }
}
