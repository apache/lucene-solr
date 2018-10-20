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
package org.apache.lucene.analysis.bg;


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
 * Test the Bulgarian Stemmer
 */
public class TestBulgarianStemmer extends BaseTokenStreamTestCase {
  /**
   * Test showing how masculine noun forms conflate. An example noun for each
   * common (and some rare) plural pattern is listed.
   */
  public void testMasculineNouns() throws IOException {
    BulgarianAnalyzer a = new BulgarianAnalyzer();
    
    // -и pattern
    assertAnalyzesTo(a, "град", new String[] {"град"});
    assertAnalyzesTo(a, "града", new String[] {"град"});
    assertAnalyzesTo(a, "градът", new String[] {"град"});
    assertAnalyzesTo(a, "градове", new String[] {"град"});
    assertAnalyzesTo(a, "градовете", new String[] {"град"});
    
    // -ове pattern
    assertAnalyzesTo(a, "народ", new String[] {"народ"});
    assertAnalyzesTo(a, "народа", new String[] {"народ"});
    assertAnalyzesTo(a, "народът", new String[] {"народ"});
    assertAnalyzesTo(a, "народи", new String[] {"народ"});
    assertAnalyzesTo(a, "народите", new String[] {"народ"});
    assertAnalyzesTo(a, "народе", new String[] {"народ"});
    
    // -ища pattern
    assertAnalyzesTo(a, "път", new String[] {"път"});
    assertAnalyzesTo(a, "пътя", new String[] {"път"});
    assertAnalyzesTo(a, "пътят", new String[] {"път"});
    assertAnalyzesTo(a, "пътища", new String[] {"път"});
    assertAnalyzesTo(a, "пътищата", new String[] {"път"});
    
    // -чета pattern
    assertAnalyzesTo(a, "градец", new String[] {"градец"});
    assertAnalyzesTo(a, "градеца", new String[] {"градец"});
    assertAnalyzesTo(a, "градецът", new String[] {"градец"});
    /* note the below forms conflate with each other, but not the rest */
    assertAnalyzesTo(a, "градовце", new String[] {"градовц"});
    assertAnalyzesTo(a, "градовцете", new String[] {"градовц"});
    
    // -овци pattern
    assertAnalyzesTo(a, "дядо", new String[] {"дяд"});
    assertAnalyzesTo(a, "дядото", new String[] {"дяд"});
    assertAnalyzesTo(a, "дядовци", new String[] {"дяд"});
    assertAnalyzesTo(a, "дядовците", new String[] {"дяд"});
    
    // -е pattern
    assertAnalyzesTo(a, "мъж", new String[] {"мъж"});
    assertAnalyzesTo(a, "мъжа", new String[] {"мъж"});
    assertAnalyzesTo(a, "мъже", new String[] {"мъж"});
    assertAnalyzesTo(a, "мъжете", new String[] {"мъж"});
    assertAnalyzesTo(a, "мъжо", new String[] {"мъж"});
    /* word is too short, will not remove -ът */
    assertAnalyzesTo(a, "мъжът", new String[] {"мъжът"});
    
    // -а pattern
    assertAnalyzesTo(a, "крак", new String[] {"крак"});
    assertAnalyzesTo(a, "крака", new String[] {"крак"});
    assertAnalyzesTo(a, "кракът", new String[] {"крак"});
    assertAnalyzesTo(a, "краката", new String[] {"крак"});
    
    // брат
    assertAnalyzesTo(a, "брат", new String[] {"брат"});
    assertAnalyzesTo(a, "брата", new String[] {"брат"});
    assertAnalyzesTo(a, "братът", new String[] {"брат"});
    assertAnalyzesTo(a, "братя", new String[] {"брат"});
    assertAnalyzesTo(a, "братята", new String[] {"брат"});
    assertAnalyzesTo(a, "брате", new String[] {"брат"});
    
    a.close();
  }
  
  /**
   * Test showing how feminine noun forms conflate
   */
  public void testFeminineNouns() throws IOException {
    BulgarianAnalyzer a = new BulgarianAnalyzer();
    
    assertAnalyzesTo(a, "вест", new String[] {"вест"});
    assertAnalyzesTo(a, "вестта", new String[] {"вест"});
    assertAnalyzesTo(a, "вести", new String[] {"вест"});
    assertAnalyzesTo(a, "вестите", new String[] {"вест"});
    
    a.close();
  }
  
  /**
   * Test showing how neuter noun forms conflate an example noun for each common
   * plural pattern is listed
   */
  public void testNeuterNouns() throws IOException {
    BulgarianAnalyzer a = new BulgarianAnalyzer();
    
    // -а pattern
    assertAnalyzesTo(a, "дърво", new String[] {"дърв"});
    assertAnalyzesTo(a, "дървото", new String[] {"дърв"});
    assertAnalyzesTo(a, "дърва", new String[] {"дърв"});
    assertAnalyzesTo(a, "дървета", new String[] {"дърв"});
    assertAnalyzesTo(a, "дървата", new String[] {"дърв"});
    assertAnalyzesTo(a, "дърветата", new String[] {"дърв"});
    
    // -та pattern
    assertAnalyzesTo(a, "море", new String[] {"мор"});
    assertAnalyzesTo(a, "морето", new String[] {"мор"});
    assertAnalyzesTo(a, "морета", new String[] {"мор"});
    assertAnalyzesTo(a, "моретата", new String[] {"мор"});
    
    // -я pattern
    assertAnalyzesTo(a, "изключение", new String[] {"изключени"});
    assertAnalyzesTo(a, "изключението", new String[] {"изключени"});
    assertAnalyzesTo(a, "изключенията", new String[] {"изключени"});
    /* note the below form in this example does not conflate with the rest */
    assertAnalyzesTo(a, "изключения", new String[] {"изключн"});
    
    a.close();
  }
  
  /**
   * Test showing how adjectival forms conflate
   */
  public void testAdjectives() throws IOException {
    BulgarianAnalyzer a = new BulgarianAnalyzer();
    assertAnalyzesTo(a, "красив", new String[] {"красив"});
    assertAnalyzesTo(a, "красивия", new String[] {"красив"});
    assertAnalyzesTo(a, "красивият", new String[] {"красив"});
    assertAnalyzesTo(a, "красива", new String[] {"красив"});
    assertAnalyzesTo(a, "красивата", new String[] {"красив"});
    assertAnalyzesTo(a, "красиво", new String[] {"красив"});
    assertAnalyzesTo(a, "красивото", new String[] {"красив"});
    assertAnalyzesTo(a, "красиви", new String[] {"красив"});
    assertAnalyzesTo(a, "красивите", new String[] {"красив"});
    a.close();
  }
  
  /**
   * Test some exceptional rules, implemented as rewrites.
   */
  public void testExceptions() throws IOException {
    BulgarianAnalyzer a = new BulgarianAnalyzer();
    
    // ци -> к
    assertAnalyzesTo(a, "собственик", new String[] {"собственик"});
    assertAnalyzesTo(a, "собственика", new String[] {"собственик"});
    assertAnalyzesTo(a, "собственикът", new String[] {"собственик"});
    assertAnalyzesTo(a, "собственици", new String[] {"собственик"});
    assertAnalyzesTo(a, "собствениците", new String[] {"собственик"});
    
    // зи -> г
    assertAnalyzesTo(a, "подлог", new String[] {"подлог"});
    assertAnalyzesTo(a, "подлога", new String[] {"подлог"});
    assertAnalyzesTo(a, "подлогът", new String[] {"подлог"});
    assertAnalyzesTo(a, "подлози", new String[] {"подлог"});
    assertAnalyzesTo(a, "подлозите", new String[] {"подлог"});
    
    // си -> х
    assertAnalyzesTo(a, "кожух", new String[] {"кожух"});
    assertAnalyzesTo(a, "кожуха", new String[] {"кожух"});
    assertAnalyzesTo(a, "кожухът", new String[] {"кожух"});
    assertAnalyzesTo(a, "кожуси", new String[] {"кожух"});
    assertAnalyzesTo(a, "кожусите", new String[] {"кожух"});
    
    // ъ deletion
    assertAnalyzesTo(a, "център", new String[] {"центр"});
    assertAnalyzesTo(a, "центъра", new String[] {"центр"});
    assertAnalyzesTo(a, "центърът", new String[] {"центр"});
    assertAnalyzesTo(a, "центрове", new String[] {"центр"});
    assertAnalyzesTo(a, "центровете", new String[] {"центр"});
    
    // е*и -> я*
    assertAnalyzesTo(a, "промяна", new String[] {"промян"});
    assertAnalyzesTo(a, "промяната", new String[] {"промян"});
    assertAnalyzesTo(a, "промени", new String[] {"промян"});
    assertAnalyzesTo(a, "промените", new String[] {"промян"});
    
    // ен -> н
    assertAnalyzesTo(a, "песен", new String[] {"песн"});
    assertAnalyzesTo(a, "песента", new String[] {"песн"});
    assertAnalyzesTo(a, "песни", new String[] {"песн"});
    assertAnalyzesTo(a, "песните", new String[] {"песн"});
    
    // -еве -> й
    // note: this is the only word i think this rule works for.
    // most -еве pluralized nouns are monosyllabic,
    // and the stemmer requires length > 6...
    assertAnalyzesTo(a, "строй", new String[] {"строй"});
    assertAnalyzesTo(a, "строеве", new String[] {"строй"});
    assertAnalyzesTo(a, "строевете", new String[] {"строй"});
    /* note the below forms conflate with each other, but not the rest */
    assertAnalyzesTo(a, "строя", new String[] {"стр"});
    assertAnalyzesTo(a, "строят", new String[] {"стр"});
    
    a.close();
  }

  public void testWithKeywordAttribute() throws IOException {
    CharArraySet set = new CharArraySet(1, true);
    set.add("строеве");
    MockTokenizer tokenStream = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    tokenStream.setReader(new StringReader("строевете строеве"));

    BulgarianStemFilter filter = new BulgarianStemFilter(
        new SetKeywordMarkerFilter(tokenStream, set));
    assertTokenStreamContents(filter, new String[] { "строй", "строеве" });
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new BulgarianStemFilter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
}
