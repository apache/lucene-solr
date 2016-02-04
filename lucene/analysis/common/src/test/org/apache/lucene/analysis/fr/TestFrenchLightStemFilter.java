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
package org.apache.lucene.analysis.fr;


import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.util.CharArraySet;

import static org.apache.lucene.analysis.VocabularyAssert.*;

/**
 * Simple tests for {@link FrenchLightStemFilter}
 */
public class TestFrenchLightStemFilter extends BaseTokenStreamTestCase {
  private Analyzer analyzer;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer source = new MockTokenizer( MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(source, new FrenchLightStemFilter(source));
      }
    };
  }
  
  @Override
  public void tearDown() throws Exception {
    analyzer.close();
    super.tearDown();
  }
  
  /** Test some examples from the paper */
  public void testExamples() throws IOException {
    checkOneTerm(analyzer, "chevaux", "cheval");
    checkOneTerm(analyzer, "cheval", "cheval");
    
    checkOneTerm(analyzer, "hiboux", "hibou");
    checkOneTerm(analyzer, "hibou", "hibou");
    
    checkOneTerm(analyzer, "chantés", "chant");
    checkOneTerm(analyzer, "chanter", "chant");
    checkOneTerm(analyzer, "chante", "chant");
    checkOneTerm(analyzer, "chant", "chant");
    
    checkOneTerm(analyzer, "baronnes", "baron");
    checkOneTerm(analyzer, "barons", "baron");
    checkOneTerm(analyzer, "baron", "baron");
    
    checkOneTerm(analyzer, "peaux", "peau");
    checkOneTerm(analyzer, "peau", "peau");
    
    checkOneTerm(analyzer, "anneaux", "aneau");
    checkOneTerm(analyzer, "anneau", "aneau");
    
    checkOneTerm(analyzer, "neveux", "neveu");
    checkOneTerm(analyzer, "neveu", "neveu");
    
    checkOneTerm(analyzer, "affreux", "afreu");
    checkOneTerm(analyzer, "affreuse", "afreu");
    
    checkOneTerm(analyzer, "investissement", "investi");
    checkOneTerm(analyzer, "investir", "investi");
    
    checkOneTerm(analyzer, "assourdissant", "asourdi");
    checkOneTerm(analyzer, "assourdir", "asourdi");
    
    checkOneTerm(analyzer, "pratiquement", "pratiqu");
    checkOneTerm(analyzer, "pratique", "pratiqu");
    
    checkOneTerm(analyzer, "administrativement", "administratif");
    checkOneTerm(analyzer, "administratif", "administratif");
    
    checkOneTerm(analyzer, "justificatrice", "justifi");
    checkOneTerm(analyzer, "justificateur", "justifi");
    checkOneTerm(analyzer, "justifier", "justifi");
    
    checkOneTerm(analyzer, "educatrice", "eduqu");
    checkOneTerm(analyzer, "eduquer", "eduqu");
    
    checkOneTerm(analyzer, "communicateur", "comuniqu");
    checkOneTerm(analyzer, "communiquer", "comuniqu");
    
    checkOneTerm(analyzer, "accompagnatrice", "acompagn");
    checkOneTerm(analyzer, "accompagnateur", "acompagn");
    
    checkOneTerm(analyzer, "administrateur", "administr");
    checkOneTerm(analyzer, "administrer", "administr");
    
    checkOneTerm(analyzer, "productrice", "product");
    checkOneTerm(analyzer, "producteur", "product");
    
    checkOneTerm(analyzer, "acheteuse", "achet");
    checkOneTerm(analyzer, "acheteur", "achet");
    
    checkOneTerm(analyzer, "planteur", "plant");
    checkOneTerm(analyzer, "plante", "plant");
    
    checkOneTerm(analyzer, "poreuse", "poreu");
    checkOneTerm(analyzer, "poreux", "poreu");
    
    checkOneTerm(analyzer, "plieuse", "plieu");
    
    checkOneTerm(analyzer, "bijoutière", "bijouti");
    checkOneTerm(analyzer, "bijoutier", "bijouti");
    
    checkOneTerm(analyzer, "caissière", "caisi");
    checkOneTerm(analyzer, "caissier", "caisi");
    
    checkOneTerm(analyzer, "abrasive", "abrasif");
    checkOneTerm(analyzer, "abrasif", "abrasif");
    
    checkOneTerm(analyzer, "folle", "fou");
    checkOneTerm(analyzer, "fou", "fou");
    
    checkOneTerm(analyzer, "personnelle", "person");
    checkOneTerm(analyzer, "personne", "person");
    
    // algo bug: too short length
    //checkOneTerm(analyzer, "personnel", "person");
    
    checkOneTerm(analyzer, "complète", "complet");
    checkOneTerm(analyzer, "complet", "complet");
    
    checkOneTerm(analyzer, "aromatique", "aromat");
    
    checkOneTerm(analyzer, "faiblesse", "faibl");
    checkOneTerm(analyzer, "faible", "faibl");
    
    checkOneTerm(analyzer, "patinage", "patin");
    checkOneTerm(analyzer, "patin", "patin");
    
    checkOneTerm(analyzer, "sonorisation", "sono");
    
    checkOneTerm(analyzer, "ritualisation", "rituel");
    checkOneTerm(analyzer, "rituel", "rituel");
    
    // algo bug: masked by rules above
    //checkOneTerm(analyzer, "colonisateur", "colon");
    
    checkOneTerm(analyzer, "nomination", "nomin");
    
    checkOneTerm(analyzer, "disposition", "dispos");
    checkOneTerm(analyzer, "dispose", "dispos");

    // SOLR-3463 : abusive compression of repeated characters in numbers
    // Trailing repeated char elision :
    checkOneTerm(analyzer, "1234555", "1234555");
    // Repeated char within numbers with more than 4 characters :
    checkOneTerm(analyzer, "12333345", "12333345");
    // Short numbers weren't affected already:
    checkOneTerm(analyzer, "1234", "1234");
    // Ensure behaviour is preserved for words!
    // Trailing repeated char elision :
    checkOneTerm(analyzer, "abcdeff", "abcdef");
    // Repeated char within words with more than 4 characters :
    checkOneTerm(analyzer, "abcccddeef", "abcdef");
    checkOneTerm(analyzer, "créées", "cre");
    // Combined letter and digit repetition
    checkOneTerm(analyzer, "22hh00", "22h00"); // 10:00pm
  }
  
  /** Test against a vocabulary from the reference impl */
  public void testVocabulary() throws IOException {
    assertVocabulary(analyzer, getDataPath("frlighttestdata.zip"), "frlight.txt");
  }
  
  public void testKeyword() throws IOException {
    final CharArraySet exclusionSet = new CharArraySet( asSet("chevaux"), false);
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer source = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        TokenStream sink = new SetKeywordMarkerFilter(source, exclusionSet);
        return new TokenStreamComponents(source, new FrenchLightStemFilter(sink));
      }
    };
    checkOneTerm(a, "chevaux", "chevaux");
    a.close();
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), analyzer, 1000*RANDOM_MULTIPLIER);
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new FrenchLightStemFilter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
}
