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
package org.apache.lucene.analysis.cz;


import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;

/**
 * Test the Czech Stemmer.
 * 
 * Note: it's algorithmic, so some stems are nonsense
 *
 */
public class TestCzechStemmer extends BaseTokenStreamTestCase {
  
  /**
   * Test showing how masculine noun forms conflate
   */
  public void testMasculineNouns() throws IOException {
    CzechAnalyzer cz = new CzechAnalyzer();
    
    /* animate ending with a hard consonant */
    assertAnalyzesTo(cz, "pán", new String[] { "pán" });
    assertAnalyzesTo(cz, "páni", new String[] { "pán" });
    assertAnalyzesTo(cz, "pánové", new String[] { "pán" });
    assertAnalyzesTo(cz, "pána", new String[] { "pán" });
    assertAnalyzesTo(cz, "pánů", new String[] { "pán" });
    assertAnalyzesTo(cz, "pánovi", new String[] { "pán" });
    assertAnalyzesTo(cz, "pánům", new String[] { "pán" });
    assertAnalyzesTo(cz, "pány", new String[] { "pán" });
    assertAnalyzesTo(cz, "páne", new String[] { "pán" });
    assertAnalyzesTo(cz, "pánech", new String[] { "pán" });
    assertAnalyzesTo(cz, "pánem", new String[] { "pán" });
    
    /* inanimate ending with hard consonant */
    assertAnalyzesTo(cz, "hrad", new String[] { "hrad" });
    assertAnalyzesTo(cz, "hradu", new String[] { "hrad" });
    assertAnalyzesTo(cz, "hrade", new String[] { "hrad" });
    assertAnalyzesTo(cz, "hradem", new String[] { "hrad" });
    assertAnalyzesTo(cz, "hrady", new String[] { "hrad" });
    assertAnalyzesTo(cz, "hradech", new String[] { "hrad" });
    assertAnalyzesTo(cz, "hradům", new String[] { "hrad" });
    assertAnalyzesTo(cz, "hradů", new String[] { "hrad" });
    
    /* animate ending with a soft consonant */
    assertAnalyzesTo(cz, "muž", new String[] { "muh" });
    assertAnalyzesTo(cz, "muži", new String[] { "muh" });
    assertAnalyzesTo(cz, "muže", new String[] { "muh" });
    assertAnalyzesTo(cz, "mužů", new String[] { "muh" });
    assertAnalyzesTo(cz, "mužům", new String[] { "muh" });
    assertAnalyzesTo(cz, "mužích", new String[] { "muh" });
    assertAnalyzesTo(cz, "mužem", new String[] { "muh" });
    
    /* inanimate ending with a soft consonant */
    assertAnalyzesTo(cz, "stroj", new String[] { "stroj" });
    assertAnalyzesTo(cz, "stroje", new String[] { "stroj" });
    assertAnalyzesTo(cz, "strojů", new String[] { "stroj" });
    assertAnalyzesTo(cz, "stroji", new String[] { "stroj" });
    assertAnalyzesTo(cz, "strojům", new String[] { "stroj" });
    assertAnalyzesTo(cz, "strojích", new String[] { "stroj" });
    assertAnalyzesTo(cz, "strojem", new String[] { "stroj" });
    
    /* ending with a */
    assertAnalyzesTo(cz, "předseda", new String[] { "předsd" });
    assertAnalyzesTo(cz, "předsedové", new String[] { "předsd" });
    assertAnalyzesTo(cz, "předsedy", new String[] { "předsd" });
    assertAnalyzesTo(cz, "předsedů", new String[] { "předsd" });
    assertAnalyzesTo(cz, "předsedovi", new String[] { "předsd" });
    assertAnalyzesTo(cz, "předsedům", new String[] { "předsd" });
    assertAnalyzesTo(cz, "předsedu", new String[] { "předsd" });
    assertAnalyzesTo(cz, "předsedo", new String[] { "předsd" });
    assertAnalyzesTo(cz, "předsedech", new String[] { "předsd" });
    assertAnalyzesTo(cz, "předsedou", new String[] { "předsd" });
    
    /* ending with e */
    assertAnalyzesTo(cz, "soudce", new String[] { "soudk" });
    assertAnalyzesTo(cz, "soudci", new String[] { "soudk" });
    assertAnalyzesTo(cz, "soudců", new String[] { "soudk" });
    assertAnalyzesTo(cz, "soudcům", new String[] { "soudk" });
    assertAnalyzesTo(cz, "soudcích", new String[] { "soudk" });
    assertAnalyzesTo(cz, "soudcem", new String[] { "soudk" });
    
    cz.close();
  }
  
  /**
   * Test showing how feminine noun forms conflate
   */
  public void testFeminineNouns() throws IOException {
    CzechAnalyzer cz = new CzechAnalyzer();
    
    /* ending with hard consonant */
    assertAnalyzesTo(cz, "kost", new String[] { "kost" });
    assertAnalyzesTo(cz, "kosti", new String[] { "kost" });
    assertAnalyzesTo(cz, "kostí", new String[] { "kost" });
    assertAnalyzesTo(cz, "kostem", new String[] { "kost" });
    assertAnalyzesTo(cz, "kostech", new String[] { "kost" });
    assertAnalyzesTo(cz, "kostmi", new String[] { "kost" });
    
    /* ending with a soft consonant */
    // note: in this example sing nom. and sing acc. don't conflate w/ the rest
    assertAnalyzesTo(cz, "píseň", new String[] { "písň" });
    assertAnalyzesTo(cz, "písně", new String[] { "písn" });
    assertAnalyzesTo(cz, "písni", new String[] { "písn" });
    assertAnalyzesTo(cz, "písněmi", new String[] { "písn" });
    assertAnalyzesTo(cz, "písních", new String[] { "písn" });
    assertAnalyzesTo(cz, "písním", new String[] { "písn" });
    
    /* ending with e */
    assertAnalyzesTo(cz, "růže", new String[] { "růh" });
    assertAnalyzesTo(cz, "růží", new String[] { "růh" });
    assertAnalyzesTo(cz, "růžím", new String[] { "růh" });
    assertAnalyzesTo(cz, "růžích", new String[] { "růh" });
    assertAnalyzesTo(cz, "růžemi", new String[] { "růh" });
    assertAnalyzesTo(cz, "růži", new String[] { "růh" });
    
    /* ending with a */
    assertAnalyzesTo(cz, "žena", new String[] { "žn" });
    assertAnalyzesTo(cz, "ženy", new String[] { "žn" });
    assertAnalyzesTo(cz, "žen", new String[] { "žn" });
    assertAnalyzesTo(cz, "ženě", new String[] { "žn" });
    assertAnalyzesTo(cz, "ženám", new String[] { "žn" });
    assertAnalyzesTo(cz, "ženu", new String[] { "žn" });
    assertAnalyzesTo(cz, "ženo", new String[] { "žn" });
    assertAnalyzesTo(cz, "ženách", new String[] { "žn" });
    assertAnalyzesTo(cz, "ženou", new String[] { "žn" });
    assertAnalyzesTo(cz, "ženami", new String[] { "žn" });
    
    cz.close();
  }

  /**
   * Test showing how neuter noun forms conflate
   */
  public void testNeuterNouns() throws IOException {
    CzechAnalyzer cz = new CzechAnalyzer();
    
    /* ending with o */
    assertAnalyzesTo(cz, "město", new String[] { "měst" });
    assertAnalyzesTo(cz, "města", new String[] { "měst" });
    assertAnalyzesTo(cz, "měst", new String[] { "měst" });
    assertAnalyzesTo(cz, "městu", new String[] { "měst" });
    assertAnalyzesTo(cz, "městům", new String[] { "měst" });
    assertAnalyzesTo(cz, "městě", new String[] { "měst" });
    assertAnalyzesTo(cz, "městech", new String[] { "měst" });
    assertAnalyzesTo(cz, "městem", new String[] { "měst" });
    assertAnalyzesTo(cz, "městy", new String[] { "měst" });
    
    /* ending with e */
    assertAnalyzesTo(cz, "moře", new String[] { "moř" });
    assertAnalyzesTo(cz, "moří", new String[] { "moř" });
    assertAnalyzesTo(cz, "mořím", new String[] { "moř" });
    assertAnalyzesTo(cz, "moři", new String[] { "moř" });
    assertAnalyzesTo(cz, "mořích", new String[] { "moř" });
    assertAnalyzesTo(cz, "mořem", new String[] { "moř" });

    /* ending with ě */
    assertAnalyzesTo(cz, "kuře", new String[] { "kuř" });
    assertAnalyzesTo(cz, "kuřata", new String[] { "kuř" });
    assertAnalyzesTo(cz, "kuřete", new String[] { "kuř" });
    assertAnalyzesTo(cz, "kuřat", new String[] { "kuř" });
    assertAnalyzesTo(cz, "kuřeti", new String[] { "kuř" });
    assertAnalyzesTo(cz, "kuřatům", new String[] { "kuř" });
    assertAnalyzesTo(cz, "kuřatech", new String[] { "kuř" });
    assertAnalyzesTo(cz, "kuřetem", new String[] { "kuř" });
    assertAnalyzesTo(cz, "kuřaty", new String[] { "kuř" });
    
    /* ending with í */
    assertAnalyzesTo(cz, "stavení", new String[] { "stavn" });
    assertAnalyzesTo(cz, "stavením", new String[] { "stavn" });
    assertAnalyzesTo(cz, "staveních", new String[] { "stavn" });
    assertAnalyzesTo(cz, "staveními", new String[] { "stavn" }); 
    
    cz.close();
  }
  
  /**
   * Test showing how adjectival forms conflate
   */
  public void testAdjectives() throws IOException {
    CzechAnalyzer cz = new CzechAnalyzer();
    
    /* ending with ý/á/é */
    assertAnalyzesTo(cz, "mladý", new String[] { "mlad" });
    assertAnalyzesTo(cz, "mladí", new String[] { "mlad" });
    assertAnalyzesTo(cz, "mladého", new String[] { "mlad" });
    assertAnalyzesTo(cz, "mladých", new String[] { "mlad" });
    assertAnalyzesTo(cz, "mladému", new String[] { "mlad" });
    assertAnalyzesTo(cz, "mladým", new String[] { "mlad" });
    assertAnalyzesTo(cz, "mladé", new String[] { "mlad" });
    assertAnalyzesTo(cz, "mladém", new String[] { "mlad" });
    assertAnalyzesTo(cz, "mladými", new String[] { "mlad" }); 
    assertAnalyzesTo(cz, "mladá", new String[] { "mlad" });
    assertAnalyzesTo(cz, "mladou", new String[] { "mlad" });

    /* ending with í */
    assertAnalyzesTo(cz, "jarní", new String[] { "jarn" });
    assertAnalyzesTo(cz, "jarního", new String[] { "jarn" });
    assertAnalyzesTo(cz, "jarních", new String[] { "jarn" });
    assertAnalyzesTo(cz, "jarnímu", new String[] { "jarn" });
    assertAnalyzesTo(cz, "jarním", new String[] { "jarn" });
    assertAnalyzesTo(cz, "jarními", new String[] { "jarn" });  
    
    cz.close();
  }
  
  /**
   * Test some possessive suffixes
   */
  public void testPossessive() throws IOException {
    CzechAnalyzer cz = new CzechAnalyzer();
    assertAnalyzesTo(cz, "Karlův", new String[] { "karl" });
    assertAnalyzesTo(cz, "jazykový", new String[] { "jazyk" });
    cz.close();
  }
  
  /**
   * Test some exceptional rules, implemented as rewrites.
   */
  public void testExceptions() throws IOException {
    CzechAnalyzer cz = new CzechAnalyzer();
    
    /* rewrite of št -> sk */
    assertAnalyzesTo(cz, "český", new String[] { "česk" });
    assertAnalyzesTo(cz, "čeští", new String[] { "česk" });
    
    /* rewrite of čt -> ck */
    assertAnalyzesTo(cz, "anglický", new String[] { "anglick" });
    assertAnalyzesTo(cz, "angličtí", new String[] { "anglick" });
    
    /* rewrite of z -> h */
    assertAnalyzesTo(cz, "kniha", new String[] { "knih" });
    assertAnalyzesTo(cz, "knize", new String[] { "knih" });
    
    /* rewrite of ž -> h */
    assertAnalyzesTo(cz, "mazat", new String[] { "mah" });
    assertAnalyzesTo(cz, "mažu", new String[] { "mah" });
    
    /* rewrite of c -> k */
    assertAnalyzesTo(cz, "kluk", new String[] { "kluk" });
    assertAnalyzesTo(cz, "kluci", new String[] { "kluk" });
    assertAnalyzesTo(cz, "klucích", new String[] { "kluk" });
    
    /* rewrite of č -> k */
    assertAnalyzesTo(cz, "hezký", new String[] { "hezk" });
    assertAnalyzesTo(cz, "hezčí", new String[] { "hezk" });
    
    /* rewrite of *ů* -> *o* */
    assertAnalyzesTo(cz, "hůl", new String[] { "hol" });
    assertAnalyzesTo(cz, "hole", new String[] { "hol" });
    
    /* rewrite of e* -> * */
    assertAnalyzesTo(cz, "deska", new String[] { "desk" });
    assertAnalyzesTo(cz, "desek", new String[] { "desk" });
    
    cz.close();
  }
  
  /**
   * Test that very short words are not stemmed.
   */
  public void testDontStem() throws IOException {
    CzechAnalyzer cz = new CzechAnalyzer();
    assertAnalyzesTo(cz, "e", new String[] { "e" });
    assertAnalyzesTo(cz, "zi", new String[] { "zi" });
    cz.close();
  }
  
  public void testWithKeywordAttribute() throws IOException {
    CharArraySet set = new CharArraySet(1, true);
    set.add("hole");
    final MockTokenizer in = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    in.setReader(new StringReader("hole desek"));
    CzechStemFilter filter = new CzechStemFilter(new SetKeywordMarkerFilter(
        in, set));
    assertTokenStreamContents(filter, new String[] { "hole", "desk" });
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new CzechStemFilter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
  
}
