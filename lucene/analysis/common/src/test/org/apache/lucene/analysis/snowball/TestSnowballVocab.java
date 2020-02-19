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
package org.apache.lucene.analysis.snowball;


import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.analysis.VocabularyAssert.*;

/**
 * Test the snowball filters against the snowball data tests
 */
public class TestSnowballVocab extends LuceneTestCase {
  /**
   * Run all languages against their snowball vocabulary tests.
   */
  public void testStemmers() throws IOException {
    try (InputStream in = getClass().getResourceAsStream("test_languages.txt")) {
      for (String datafile : WordlistLoader.getLines(in, StandardCharsets.UTF_8)) {
        String language = "" + Character.toUpperCase(datafile.charAt(0)) + datafile.substring(1);
        assertCorrectOutput(language, datafile + ".zip");
      }
    }
  }
    
  /**
   * For the supplied language, run the stemmer against all strings in voc.txt
   * The output should be the same as the string in output.txt
   */
  private void assertCorrectOutput(final String snowballLanguage, String zipfile)
      throws IOException {
    if (VERBOSE) System.out.println("checking snowball language: " + snowballLanguage);
    
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer t = new KeywordTokenizer();
        return new TokenStreamComponents(t, new SnowballFilter(t, snowballLanguage));
      }  
    };
    
    assertVocabulary(a, getDataPath(zipfile), "voc.txt", "output.txt");
    a.close();
  }
}
