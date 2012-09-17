package org.apache.lucene.analysis.fr;

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

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

/**
 * Test case for FrenchAnalyzer.
 *
 */

public class TestFrenchAnalyzer extends BaseTokenStreamTestCase {

  public void testAnalyzer() throws Exception {
    FrenchAnalyzer fa = new FrenchAnalyzer(TEST_VERSION_CURRENT);

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

  }

  /**
   * @deprecated (3.1) remove this test for Lucene 5.0
   */
  @Deprecated
  public void testAnalyzer30() throws Exception {
      FrenchAnalyzer fa = new FrenchAnalyzer(Version.LUCENE_30);

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
        new String[] { "mot", "entreguillemet" });

      // let's do some french specific tests now

      /* 1. couldn't resist
       I would expect this to stay one term as in French the minus
      sign is often used for composing words */
      assertAnalyzesTo(
        fa,
        "Jean-François",
        new String[] { "jean", "françois" });

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
          "habit",
          "chist",
          "élément",
          "captif" });

      // some verbs
      assertAnalyzesTo(
        fa,
        "finissions souffrirent rugissante",
        new String[] { "fin", "souffr", "rug" });

      // some everything else
      // aujourd'hui stays one term which is OK
      assertAnalyzesTo(
        fa,
        "C3PO aujourd'hui oeuf ïâöûàä anticonstitutionnellement Java++ ",
        new String[] {
          "c3po",
          "aujourd'hui",
          "oeuf",
          "ïâöûàä",
          "anticonstitutionnel",
          "jav" });

      // some more everything else
      // here 1940-1945 stays as one term, 1940:1945 not ?
      assertAnalyzesTo(
        fa,
        "33Bis 1940-1945 1940:1945 (---i+++)*",
        new String[] { "33bis", "1940-1945", "1940", "1945", "i" });

    }

  public void testReusableTokenStream() throws Exception {
    FrenchAnalyzer fa = new FrenchAnalyzer(TEST_VERSION_CURRENT);
    // stopwords
      assertAnalyzesToReuse(
          fa,
          "le la chien les aux chat du des à cheval",
          new String[] { "chien", "chat", "cheval" });

      // some nouns and adjectives
      assertAnalyzesToReuse(
          fa,
          "lances chismes habitable chiste éléments captifs",
          new String[] {
              "lanc",
              "chism",
              "habitabl",
              "chist",
              "element",
              "captif" });
  }

  public void testExclusionTableViaCtor() throws Exception {
    CharArraySet set = new CharArraySet(TEST_VERSION_CURRENT, 1, true);
    set.add("habitable");
    FrenchAnalyzer fa = new FrenchAnalyzer(TEST_VERSION_CURRENT,
        CharArraySet.EMPTY_SET, set);
    assertAnalyzesToReuse(fa, "habitable chiste", new String[] { "habitable",
        "chist" });

    fa = new FrenchAnalyzer(TEST_VERSION_CURRENT, CharArraySet.EMPTY_SET, set);
    assertAnalyzesTo(fa, "habitable chiste", new String[] { "habitable",
        "chist" });
  }
  
  public void testElision() throws Exception {
    FrenchAnalyzer fa = new FrenchAnalyzer(TEST_VERSION_CURRENT);
    assertAnalyzesTo(fa, "voir l'embrouille", new String[] { "voir", "embrouil" });
  }
  
  /**
   * Prior to 3.1, this analyzer had no lowercase filter.
   * stopwords were case sensitive. Preserve this for back compat.
   * @deprecated (3.1) Remove this test in Lucene 5.0
   */
  @Deprecated
  public void testBuggyStopwordsCasing() throws IOException {
    FrenchAnalyzer a = new FrenchAnalyzer(Version.LUCENE_30);
    assertAnalyzesTo(a, "Votre", new String[] { "votr" });
  }
  
  /**
   * Test that stopwords are not case sensitive
   */
  public void testStopwordsCasing() throws IOException {
    FrenchAnalyzer a = new FrenchAnalyzer(Version.LUCENE_31);
    assertAnalyzesTo(a, "Votre", new String[] { });
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), new FrenchAnalyzer(TEST_VERSION_CURRENT), 1000*RANDOM_MULTIPLIER);
  }
  
  /** test accent-insensitive */
  public void testAccentInsensitive() throws Exception {
    Analyzer a = new FrenchAnalyzer(TEST_VERSION_CURRENT);
    checkOneTermReuse(a, "sécuritaires", "securitair");
    checkOneTermReuse(a, "securitaires", "securitair");
  }
}
