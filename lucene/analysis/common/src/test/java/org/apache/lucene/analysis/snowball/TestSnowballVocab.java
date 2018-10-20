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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;

import static org.apache.lucene.analysis.VocabularyAssert.*;

/**
 * Test the snowball filters against the snowball data tests
 */
@Slow
public class TestSnowballVocab extends LuceneTestCase {
  /**
   * Run all languages against their snowball vocabulary tests.
   */
  public void testStemmers() throws IOException {
    assertCorrectOutput("Danish", "danish");
    assertCorrectOutput("Dutch", "dutch");
    assertCorrectOutput("English", "english");
    assertCorrectOutput("Finnish", "finnish");
    assertCorrectOutput("French", "french");
    assertCorrectOutput("German", "german");
    assertCorrectOutput("German2", "german2");
    assertCorrectOutput("Hungarian", "hungarian");
    assertCorrectOutput("Italian", "italian");
    assertCorrectOutput("Kp", "kraaij_pohlmann");
    assertCorrectOutput("Lovins", "lovins");
    assertCorrectOutput("Norwegian", "norwegian");
    assertCorrectOutput("Porter", "porter");
    assertCorrectOutput("Portuguese", "portuguese");
    assertCorrectOutput("Romanian", "romanian");
    assertCorrectOutput("Russian", "russian");
    assertCorrectOutput("Spanish", "spanish");
    assertCorrectOutput("Swedish", "swedish");
    assertCorrectOutput("Turkish", "turkish");
  }
    
  /**
   * For the supplied language, run the stemmer against all strings in voc.txt
   * The output should be the same as the string in output.txt
   */
  private void assertCorrectOutput(final String snowballLanguage, String dataDirectory)
      throws IOException {
    if (VERBOSE) System.out.println("checking snowball language: " + snowballLanguage);
    
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer t = new KeywordTokenizer();
        return new TokenStreamComponents(t, new SnowballFilter(t, snowballLanguage));
      }  
    };
    
    assertVocabulary(a, getDataPath("TestSnowballVocabData.zip"), 
        dataDirectory + "/voc.txt", dataDirectory + "/output.txt");
    a.close();
  }
}
