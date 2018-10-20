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
import org.apache.lucene.analysis.CharArraySet;

/**
 * Test case for FrenchAnalyzer.
 *
 */

public class TestFrenchAnalyzer extends BaseTokenStreamTestCase {

  public void testAnalyzer() throws Exception {
    FrenchAnalyzer fa = new FrenchAnalyzer();
  
    assertAnalyzesTo(fa, "", new String[] {
    });

    assertAnalyzesTo(
      fa,
      "chien chat cheval",
      new String[] { "chien", "chat", "cheval" });

    assertAnalyzesTo(
      fa,
      "chien CHAT CHEVAL",
      new String[] { "chien", "chat", "cheval" });

    assertAnalyzesTo(
      fa,
      "  chien  ,? + = -  CHAT /: > CHEVAL",
      new String[] { "chien", "chat", "cheval" });

    assertAnalyzesTo(fa, "chien++", new String[] { "chien" });

    assertAnalyzesTo(
      fa,
      "mot \"entreguillemet\"",
      new String[] { "mot", "entreguilemet" });

     // let's do some french specific tests now   
          /* 1. couldn't resist
      I would expect this to stay one term as in French the minus 
    sign is often used for composing words */
    assertAnalyzesTo(
      fa,
      "Jean-François",
      new String[] { "jean", "francoi" });

    // 2. stopwords
    assertAnalyzesTo(
      fa,
      "le la chien les aux chat du des à cheval",
      new String[] { "chien", "chat", "cheval" });

    // some nouns and adjectives
    assertAnalyzesTo(
      fa,
      "lances chismes habitable chiste éléments captifs",
      new String[] {
        "lanc",
        "chism",
        "habitabl",
        "chist",
        "element",
        "captif" });

    // some verbs
    assertAnalyzesTo(
      fa,
      "finissions souffrirent rugissante",
      new String[] { "finision", "soufrirent", "rugisant" });

    // some everything else
    // aujourd'hui stays one term which is OK
    assertAnalyzesTo(
      fa,
      "C3PO aujourd'hui oeuf ïâöûàä anticonstitutionnellement Java++ ",
      new String[] {
        "c3po",
        "aujourd'hui",
        "oeuf",
        "ïaöuaä",
        "anticonstitutionel",
        "java" });

    // some more everything else
    // here 1940-1945 stays as one term, 1940:1945 not ?
    assertAnalyzesTo(
      fa,
      "33Bis 1940-1945 1940:1945 (---i+++)*",
      new String[] { "33bi", "1940", "1945", "1940", "1945", "i" });
    fa.close();
  }
  
  public void testReusableTokenStream() throws Exception {
    FrenchAnalyzer fa = new FrenchAnalyzer();
    // stopwords
      assertAnalyzesTo(
          fa,
          "le la chien les aux chat du des à cheval",
          new String[] { "chien", "chat", "cheval" });

      // some nouns and adjectives
      assertAnalyzesTo(
          fa,
          "lances chismes habitable chiste éléments captifs",
          new String[] {
              "lanc",
              "chism",
              "habitabl",
              "chist",
              "element",
              "captif" });
      fa.close();
  }

  public void testExclusionTableViaCtor() throws Exception {
    CharArraySet set = new CharArraySet( 1, true);
    set.add("habitable");
    FrenchAnalyzer fa = new FrenchAnalyzer(
        CharArraySet.EMPTY_SET, set);
    assertAnalyzesTo(fa, "habitable chiste", new String[] { "habitable",
        "chist" });
    fa.close();

    fa = new FrenchAnalyzer( CharArraySet.EMPTY_SET, set);
    assertAnalyzesTo(fa, "habitable chiste", new String[] { "habitable",
        "chist" });
    fa.close();
  }
  
  public void testElision() throws Exception {
    FrenchAnalyzer fa = new FrenchAnalyzer();
    assertAnalyzesTo(fa, "voir l'embrouille", new String[] { "voir", "embrouil" });
    fa.close();
  }
  
  /**
   * Test that stopwords are not case sensitive
   */
  public void testStopwordsCasing() throws IOException {
    FrenchAnalyzer a = new FrenchAnalyzer();
    assertAnalyzesTo(a, "Votre", new String[] { });
    a.close();
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer a = new FrenchAnalyzer();
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER);
    a.close();
  }
  
  /** test accent-insensitive */
  public void testAccentInsensitive() throws Exception {
    Analyzer a = new FrenchAnalyzer();
    checkOneTerm(a, "sécuritaires", "securitair");
    checkOneTerm(a, "securitaires", "securitair");
    a.close();
  }
}
