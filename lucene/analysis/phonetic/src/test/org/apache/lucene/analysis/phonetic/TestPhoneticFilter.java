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
package org.apache.lucene.analysis.phonetic;

import java.io.IOException;
import java.io.StringReader;

import org.apache.commons.codec.Encoder;
import org.apache.commons.codec.language.Caverphone2;
import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.codec.language.Metaphone;
import org.apache.commons.codec.language.Nysiis;
import org.apache.commons.codec.language.RefinedSoundex;
import org.apache.commons.codec.language.Soundex;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

/**
 * Tests {@link PhoneticFilter}
 */
public class TestPhoneticFilter extends BaseTokenStreamTestCase {
   
  public void testAlgorithms() throws Exception {
    assertAlgorithm(new Metaphone(), true, "aaa bbb ccc easgasg",
        new String[] { "A", "aaa", "B", "bbb", "KKK", "ccc", "ESKS", "easgasg" });
    assertAlgorithm(new Metaphone(), false, "aaa bbb ccc easgasg",
        new String[] { "A", "B", "KKK", "ESKS" });
    
    assertAlgorithm(new DoubleMetaphone(), true, "aaa bbb ccc easgasg",
        new String[] { "A", "aaa", "PP", "bbb", "KK", "ccc", "ASKS", "easgasg" });
    assertAlgorithm(new DoubleMetaphone(), false, "aaa bbb ccc easgasg",
        new String[] { "A", "PP", "KK", "ASKS" });
    
    assertAlgorithm(new Soundex(), true, "aaa bbb ccc easgasg",
        new String[] { "A000", "aaa", "B000", "bbb", "C000", "ccc", "E220", "easgasg" });
    assertAlgorithm(new Soundex(), false, "aaa bbb ccc easgasg",
        new String[] { "A000", "B000", "C000", "E220" });
    
    assertAlgorithm(new RefinedSoundex(), true, "aaa bbb ccc easgasg",
        new String[] { "A0", "aaa", "B1", "bbb", "C3", "ccc", "E034034", "easgasg" });
    assertAlgorithm(new RefinedSoundex(), false, "aaa bbb ccc easgasg",
        new String[] { "A0", "B1", "C3", "E034034" });
    
    assertAlgorithm(new Caverphone2(), true, "Darda Karleen Datha Carlene",
        new String[] { "TTA1111111", "Darda", "KLN1111111", "Karleen", 
          "TTA1111111", "Datha", "KLN1111111", "Carlene" });
    assertAlgorithm(new Caverphone2(), false, "Darda Karleen Datha Carlene",
        new String[] { "TTA1111111", "KLN1111111", "TTA1111111", "KLN1111111" });

    assertAlgorithm(new Nysiis(), true, "aaa bbb ccc easgasg",
        new String[] { "A", "aaa", "B", "bbb", "C", "ccc", "EASGAS", "easgasg" });
    assertAlgorithm(new Nysiis(), false, "aaa bbb ccc easgasg",
        new String[] { "A", "B", "C", "EASGAS" });
  }

  
  static void assertAlgorithm(Encoder encoder, boolean inject, String input,
      String[] expected) throws Exception {
    Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    tokenizer.setReader(new StringReader(input));
    PhoneticFilter filter = new PhoneticFilter(tokenizer, encoder, inject);
    assertTokenStreamContents(filter, expected);
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws IOException {
    Encoder encoders[] = new Encoder[] {
      new Metaphone(), new DoubleMetaphone(), new Soundex(), new RefinedSoundex(), new Caverphone2()
    };
    
    for (final Encoder e : encoders) {
      Analyzer a = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
          return new TokenStreamComponents(tokenizer, new PhoneticFilter(tokenizer, e, false));
        }   
      };
      
      checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER);
      a.close();
      
      Analyzer b = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
          return new TokenStreamComponents(tokenizer, new PhoneticFilter(tokenizer, e, false));
        }   
      };
      
      checkRandomData(random(), b, 1000*RANDOM_MULTIPLIER);
      b.close();
    }
  }
  
  public void testEmptyTerm() throws IOException {
    Encoder encoders[] = new Encoder[] {
        new Metaphone(), new DoubleMetaphone(), new Soundex(), new RefinedSoundex(), new Caverphone2()
    };
    for (final Encoder e : encoders) {
      Analyzer a = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tokenizer = new KeywordTokenizer();
          return new TokenStreamComponents(tokenizer, new PhoneticFilter(tokenizer, e, random().nextBoolean()));
        }
      };
      checkOneTerm(a, "", "");
      a.close();
    }
  }
}
