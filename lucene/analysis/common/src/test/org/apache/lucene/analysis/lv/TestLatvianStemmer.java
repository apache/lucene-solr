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
package org.apache.lucene.analysis.lv;


import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

/**
 * Basic tests for {@link LatvianStemmer}
 */
public class TestLatvianStemmer extends BaseTokenStreamTestCase {
  private Analyzer a;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new LatvianStemFilter(tokenizer));
      }
    };
  }
  
  @Override
  public void tearDown() throws Exception {
    a.close();
    super.tearDown();
  }
  
  public void testNouns1() throws IOException {
    // decl. I
    checkOneTerm(a, "tēvs",   "tēv"); // nom. sing.
    checkOneTerm(a, "tēvi",   "tēv"); // nom. pl.
    checkOneTerm(a, "tēva",   "tēv"); // gen. sing.
    checkOneTerm(a, "tēvu",   "tēv"); // gen. pl.
    checkOneTerm(a, "tēvam",  "tēv"); // dat. sing.
    checkOneTerm(a, "tēviem", "tēv"); // dat. pl.
    checkOneTerm(a, "tēvu",   "tēv"); // acc. sing.
    checkOneTerm(a, "tēvus",  "tēv"); // acc. pl.
    checkOneTerm(a, "tēvā",   "tēv"); // loc. sing.
    checkOneTerm(a, "tēvos",  "tēv"); // loc. pl.
    checkOneTerm(a, "tēvs",   "tēv"); // voc. sing.
    checkOneTerm(a, "tēvi",   "tēv"); // voc. pl.
  }
  
  /**
   * decl II nouns with (s,t) -&gt; š and (d,z) -&gt; ž
   * palatalization will generally conflate to two stems
   * due to the ambiguity (plural and singular).
   */
  public void testNouns2() throws IOException {
    // decl. II
    
    // c -> č palatalization
    checkOneTerm(a, "lācis",  "lāc"); // nom. sing.
    checkOneTerm(a, "lāči",   "lāc"); // nom. pl.
    checkOneTerm(a, "lāča",   "lāc"); // gen. sing.
    checkOneTerm(a, "lāču",   "lāc"); // gen. pl.
    checkOneTerm(a, "lācim",  "lāc"); // dat. sing.
    checkOneTerm(a, "lāčiem", "lāc"); // dat. pl.
    checkOneTerm(a, "lāci",   "lāc"); // acc. sing.
    checkOneTerm(a, "lāčus",  "lāc"); // acc. pl.
    checkOneTerm(a, "lācī",   "lāc"); // loc. sing.
    checkOneTerm(a, "lāčos",  "lāc"); // loc. pl.
    checkOneTerm(a, "lāci",   "lāc"); // voc. sing.
    checkOneTerm(a, "lāči",   "lāc"); // voc. pl.
    
    // n -> ņ palatalization
    checkOneTerm(a, "akmens",   "akmen"); // nom. sing.
    checkOneTerm(a, "akmeņi",   "akmen"); // nom. pl.
    checkOneTerm(a, "akmens",   "akmen"); // gen. sing.
    checkOneTerm(a, "akmeņu",   "akmen"); // gen. pl.
    checkOneTerm(a, "akmenim",  "akmen"); // dat. sing.
    checkOneTerm(a, "akmeņiem", "akmen"); // dat. pl.
    checkOneTerm(a, "akmeni",   "akmen"); // acc. sing.
    checkOneTerm(a, "akmeņus",  "akmen"); // acc. pl.
    checkOneTerm(a, "akmenī",   "akmen"); // loc. sing.
    checkOneTerm(a, "akmeņos",  "akmen"); // loc. pl.
    checkOneTerm(a, "akmens",   "akmen"); // voc. sing.
    checkOneTerm(a, "akmeņi",   "akmen"); // voc. pl.
    
    // no palatalization
    checkOneTerm(a, "kurmis",   "kurm"); // nom. sing.
    checkOneTerm(a, "kurmji",   "kurm"); // nom. pl.
    checkOneTerm(a, "kurmja",   "kurm"); // gen. sing.
    checkOneTerm(a, "kurmju",   "kurm"); // gen. pl.
    checkOneTerm(a, "kurmim",   "kurm"); // dat. sing.
    checkOneTerm(a, "kurmjiem", "kurm"); // dat. pl.
    checkOneTerm(a, "kurmi",    "kurm"); // acc. sing.
    checkOneTerm(a, "kurmjus",  "kurm"); // acc. pl.
    checkOneTerm(a, "kurmī",    "kurm"); // loc. sing.
    checkOneTerm(a, "kurmjos",  "kurm"); // loc. pl.
    checkOneTerm(a, "kurmi",    "kurm"); // voc. sing.
    checkOneTerm(a, "kurmji",   "kurm"); // voc. pl.
  }
  
  public void testNouns3() throws IOException {
    // decl III
    checkOneTerm(a, "lietus",  "liet"); // nom. sing.
    checkOneTerm(a, "lieti",   "liet"); // nom. pl.
    checkOneTerm(a, "lietus",  "liet"); // gen. sing.
    checkOneTerm(a, "lietu",   "liet"); // gen. pl.
    checkOneTerm(a, "lietum",  "liet"); // dat. sing.
    checkOneTerm(a, "lietiem", "liet"); // dat. pl.
    checkOneTerm(a, "lietu",   "liet"); // acc. sing.
    checkOneTerm(a, "lietus",  "liet"); // acc. pl.
    checkOneTerm(a, "lietū",   "liet"); // loc. sing.
    checkOneTerm(a, "lietos",  "liet"); // loc. pl.
    checkOneTerm(a, "lietus",  "liet"); // voc. sing.
    checkOneTerm(a, "lieti",   "liet"); // voc. pl.
  }
  
  public void testNouns4() throws IOException {
    // decl IV
    checkOneTerm(a, "lapa",  "lap"); // nom. sing.
    checkOneTerm(a, "lapas", "lap"); // nom. pl.
    checkOneTerm(a, "lapas", "lap"); // gen. sing.
    checkOneTerm(a, "lapu",  "lap"); // gen. pl.
    checkOneTerm(a, "lapai", "lap"); // dat. sing.
    checkOneTerm(a, "lapām", "lap"); // dat. pl.
    checkOneTerm(a, "lapu",  "lap"); // acc. sing.
    checkOneTerm(a, "lapas", "lap"); // acc. pl.
    checkOneTerm(a, "lapā",  "lap"); // loc. sing.
    checkOneTerm(a, "lapās", "lap"); // loc. pl.
    checkOneTerm(a, "lapa",  "lap"); // voc. sing.
    checkOneTerm(a, "lapas", "lap"); // voc. pl.
    
    checkOneTerm(a, "puika",  "puik"); // nom. sing.
    checkOneTerm(a, "puikas", "puik"); // nom. pl.
    checkOneTerm(a, "puikas", "puik"); // gen. sing.
    checkOneTerm(a, "puiku",  "puik"); // gen. pl.
    checkOneTerm(a, "puikam", "puik"); // dat. sing.
    checkOneTerm(a, "puikām", "puik"); // dat. pl.
    checkOneTerm(a, "puiku",  "puik"); // acc. sing.
    checkOneTerm(a, "puikas", "puik"); // acc. pl.
    checkOneTerm(a, "puikā",  "puik"); // loc. sing.
    checkOneTerm(a, "puikās", "puik"); // loc. pl.
    checkOneTerm(a, "puika",  "puik"); // voc. sing.
    checkOneTerm(a, "puikas", "puik"); // voc. pl.
  }
  
  /**
   * Genitive plural forms with (s,t) -&gt; š and (d,z) -&gt; ž
   * will not conflate due to ambiguity.
   */
  public void testNouns5() throws IOException {
    // decl V
    // l -> ļ palatalization
    checkOneTerm(a, "egle",  "egl"); // nom. sing.
    checkOneTerm(a, "egles", "egl"); // nom. pl.
    checkOneTerm(a, "egles", "egl"); // gen. sing.
    checkOneTerm(a, "egļu",  "egl"); // gen. pl.
    checkOneTerm(a, "eglei", "egl"); // dat. sing.
    checkOneTerm(a, "eglēm", "egl"); // dat. pl.
    checkOneTerm(a, "egli",  "egl"); // acc. sing.
    checkOneTerm(a, "egles", "egl"); // acc. pl.
    checkOneTerm(a, "eglē",  "egl"); // loc. sing.
    checkOneTerm(a, "eglēs", "egl"); // loc. pl.
    checkOneTerm(a, "egle",  "egl"); // voc. sing.
    checkOneTerm(a, "egles", "egl"); // voc. pl.
  }
  
  public void testNouns6() throws IOException {
    // decl VI
    
    // no palatalization
    checkOneTerm(a, "govs",  "gov"); // nom. sing.
    checkOneTerm(a, "govis", "gov"); // nom. pl.
    checkOneTerm(a, "govs",  "gov"); // gen. sing.
    checkOneTerm(a, "govju", "gov"); // gen. pl.
    checkOneTerm(a, "govij", "gov"); // dat. sing.
    checkOneTerm(a, "govīm", "gov"); // dat. pl.
    checkOneTerm(a, "govi ", "gov"); // acc. sing.
    checkOneTerm(a, "govis", "gov"); // acc. pl.
    checkOneTerm(a, "govi ", "gov"); // inst. sing.
    checkOneTerm(a, "govīm", "gov"); // inst. pl.
    checkOneTerm(a, "govī",  "gov"); // loc. sing.
    checkOneTerm(a, "govīs", "gov"); // loc. pl.
    checkOneTerm(a, "govs",  "gov"); // voc. sing.
    checkOneTerm(a, "govis", "gov"); // voc. pl.
  }
  
  public void testAdjectives() throws IOException {
    checkOneTerm(a, "zils",     "zil"); // indef. nom. masc. sing.
    checkOneTerm(a, "zilais",   "zil"); // def. nom. masc. sing.
    checkOneTerm(a, "zili",     "zil"); // indef. nom. masc. pl.
    checkOneTerm(a, "zilie",    "zil"); // def. nom. masc. pl.
    checkOneTerm(a, "zila",     "zil"); // indef. nom. fem. sing.
    checkOneTerm(a, "zilā",     "zil"); // def. nom. fem. sing.
    checkOneTerm(a, "zilas",    "zil"); // indef. nom. fem. pl.
    checkOneTerm(a, "zilās",    "zil"); // def. nom. fem. pl.
    checkOneTerm(a, "zila",     "zil"); // indef. gen. masc. sing.
    checkOneTerm(a, "zilā",     "zil"); // def. gen. masc. sing.
    checkOneTerm(a, "zilu",     "zil"); // indef. gen. masc. pl.
    checkOneTerm(a, "zilo",     "zil"); // def. gen. masc. pl.
    checkOneTerm(a, "zilas",    "zil"); // indef. gen. fem. sing.
    checkOneTerm(a, "zilās",    "zil"); // def. gen. fem. sing.
    checkOneTerm(a, "zilu",     "zil"); // indef. gen. fem. pl.
    checkOneTerm(a, "zilo",     "zil"); // def. gen. fem. pl.
    checkOneTerm(a, "zilam",    "zil"); // indef. dat. masc. sing.
    checkOneTerm(a, "zilajam",  "zil"); // def. dat. masc. sing.
    checkOneTerm(a, "ziliem",   "zil"); // indef. dat. masc. pl.
    checkOneTerm(a, "zilajiem", "zil"); // def. dat. masc. pl.
    checkOneTerm(a, "zilai",    "zil"); // indef. dat. fem. sing.
    checkOneTerm(a, "zilajai",  "zil"); // def. dat. fem. sing.
    checkOneTerm(a, "zilām",    "zil"); // indef. dat. fem. pl.
    checkOneTerm(a, "zilajām",  "zil"); // def. dat. fem. pl.
    checkOneTerm(a, "zilu",     "zil"); // indef. acc. masc. sing.
    checkOneTerm(a, "zilo",     "zil"); // def. acc. masc. sing.
    checkOneTerm(a, "zilus",    "zil"); // indef. acc. masc. pl.
    checkOneTerm(a, "zilos",    "zil"); // def. acc. masc. pl.
    checkOneTerm(a, "zilu",     "zil"); // indef. acc. fem. sing.
    checkOneTerm(a, "zilo",     "zil"); // def. acc. fem. sing.
    checkOneTerm(a, "zilās",    "zil"); // indef. acc. fem. pl.
    checkOneTerm(a, "zilās",    "zil"); // def. acc. fem. pl.
    checkOneTerm(a, "zilā",     "zil"); // indef. loc. masc. sing.
    checkOneTerm(a, "zilajā",   "zil"); // def. loc. masc. sing.
    checkOneTerm(a, "zilos",    "zil"); // indef. loc. masc. pl.
    checkOneTerm(a, "zilajos",  "zil"); // def. loc. masc. pl.
    checkOneTerm(a, "zilā",     "zil"); // indef. loc. fem. sing.
    checkOneTerm(a, "zilajā",   "zil"); // def. loc. fem. sing.
    checkOneTerm(a, "zilās",    "zil"); // indef. loc. fem. pl.
    checkOneTerm(a, "zilajās",  "zil"); // def. loc. fem. pl.
    checkOneTerm(a, "zilais",   "zil"); // voc. masc. sing.
    checkOneTerm(a, "zilie",    "zil"); // voc. masc. pl.
    checkOneTerm(a, "zilā",     "zil"); // voc. fem. sing.
    checkOneTerm(a, "zilās",    "zil"); // voc. fem. pl.
  }
  
  /**
   * Note: we intentionally don't handle the ambiguous
   * (s,t) -&gt; š and (d,z) -&gt; ž
   */
  public void testPalatalization() throws IOException {
    checkOneTerm(a, "krāsns", "krāsn"); // nom. sing.
    checkOneTerm(a, "krāšņu", "krāsn"); // gen. pl.
    checkOneTerm(a, "zvaigzne", "zvaigzn"); // nom. sing.
    checkOneTerm(a, "zvaigžņu", "zvaigzn"); // gen. pl.
    checkOneTerm(a, "kāpslis", "kāpsl"); // nom. sing.
    checkOneTerm(a, "kāpšļu",  "kāpsl"); // gen. pl.
    checkOneTerm(a, "zizlis", "zizl"); // nom. sing.
    checkOneTerm(a, "zižļu",  "zizl"); // gen. pl.
    checkOneTerm(a, "vilnis", "viln"); // nom. sing.
    checkOneTerm(a, "viļņu",  "viln"); // gen. pl.
    checkOneTerm(a, "lelle", "lell"); // nom. sing.
    checkOneTerm(a, "leļļu", "lell"); // gen. pl.
    checkOneTerm(a, "pinne", "pinn"); // nom. sing.
    checkOneTerm(a, "piņņu", "pinn"); // gen. pl.
    checkOneTerm(a, "rīkste", "rīkst"); // nom. sing.
    checkOneTerm(a, "rīkšu",  "rīkst"); // gen. pl.
  }
  
  /**
   * Test some length restrictions, we require a 3+ char stem,
   * with at least one vowel.
   */
  public void testLength() throws IOException {
    checkOneTerm(a, "usa", "usa"); // length
    checkOneTerm(a, "60ms", "60ms"); // vowel count
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new LatvianStemFilter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
}
