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
package org.apache.lucene.analysis.hi;


import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

/**
 * Test HindiStemmer
 */
public class TestHindiStemmer extends BaseTokenStreamTestCase {
  /**
   * Test masc noun inflections
   */
  public void testMasculineNouns() throws IOException {
    check("लडका", "लडक");
    check("लडके", "लडक");
    check("लडकों", "लडक");
    
    check("गुरु", "गुर");
    check("गुरुओं", "गुर");
    
    check("दोस्त", "दोस्त");
    check("दोस्तों", "दोस्त");
  }
  
  /**
   * Test feminine noun inflections
   */
  public void testFeminineNouns() throws IOException {
    check("लडकी", "लडक");
    check("लडकियों", "लडक");
    
    check("किताब", "किताब");
    check("किताबें", "किताब");
    check("किताबों", "किताब");
    
    check("आध्यापीका", "आध्यापीक");
    check("आध्यापीकाएं", "आध्यापीक");
    check("आध्यापीकाओं", "आध्यापीक");
  }
  
  /**
   * Test some verb forms
   */
  public void testVerbs() throws IOException {
    check("खाना", "खा");
    check("खाता", "खा");
    check("खाती", "खा");
    check("खा", "खा");
  }
  
  /**
   * From the paper: since the suffix list for verbs includes AI, awA and anI,
   * additional suffixes had to be added to the list for noun/adjectives
   * ending with these endings.
   */
  public void testExceptions() throws IOException {
    check("कठिनाइयां", "कठिन");
    check("कठिन", "कठिन");
  }
  
  private void check(String input, String output) throws IOException {
    Tokenizer tokenizer = whitespaceMockTokenizer(input);
    TokenFilter tf = new HindiStemFilter(tokenizer);
    assertTokenStreamContents(tf, new String[] { output });
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new HindiStemFilter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
}
