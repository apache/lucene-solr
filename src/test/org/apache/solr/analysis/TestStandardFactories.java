package org.apache.solr.analysis;

/**
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

import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WhitespaceTokenizer;

/**
 * Simple tests to ensure the standard lucene factories are working.
 */
public class TestStandardFactories extends BaseTokenTestCase {
  /**
   * Test StandardTokenizerFactory
   */
  public void testStandardTokenizer() throws Exception {
    Reader reader = new StringReader("What's this thing do?");
    StandardTokenizerFactory factory = new StandardTokenizerFactory();
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] {"What's", "this", "thing", "do" });
  }
  
  /**
   * Test StandardFilterFactory
   */
  public void testStandardFilter() throws Exception {
    Reader reader = new StringReader("What's this thing do?");
    StandardTokenizerFactory factory = new StandardTokenizerFactory();
    StandardFilterFactory filterFactory = new StandardFilterFactory();
    Tokenizer tokenizer = factory.create(reader);
    TokenStream stream = filterFactory.create(tokenizer);
    assertTokenStreamContents(stream, 
        new String[] {"What", "this", "thing", "do"});
  }
  
  /**
   * Test KeywordTokenizerFactory
   */
  public void testKeywordTokenizer() throws Exception {
    Reader reader = new StringReader("What's this thing do?");
    KeywordTokenizerFactory factory = new KeywordTokenizerFactory();
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] {"What's this thing do?"});
  }
  
  /**
   * Test WhitespaceTokenizerFactory
   */
  public void testWhitespaceTokenizer() throws Exception {
    Reader reader = new StringReader("What's this thing do?");
    WhitespaceTokenizerFactory factory = new WhitespaceTokenizerFactory();
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] {"What's", "this", "thing", "do?"});
  }
  
  /**
   * Test LetterTokenizerFactory
   */
  public void testLetterTokenizer() throws Exception {
    Reader reader = new StringReader("What's this thing do?");
    LetterTokenizerFactory factory = new LetterTokenizerFactory();
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] {"What", "s", "this", "thing", "do"});
  }
  
  /**
   * Test LowerCaseTokenizerFactory
   */
  public void testLowerCaseTokenizer() throws Exception {
    Reader reader = new StringReader("What's this thing do?");
    LowerCaseTokenizerFactory factory = new LowerCaseTokenizerFactory();
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] {"what", "s", "this", "thing", "do"});
  }
  
  /**
   * Ensure the ASCIIFoldingFilterFactory works
   */
  public void testASCIIFolding() throws Exception {
    Reader reader = new StringReader("Česká");
    Tokenizer tokenizer = new WhitespaceTokenizer(reader);
    ASCIIFoldingFilterFactory factory = new ASCIIFoldingFilterFactory();
    TokenStream stream = factory.create(tokenizer);
    assertTokenStreamContents(stream, new String[] { "Ceska" });
  }
  
  /**
   * Ensure the ISOLatin1AccentFilterFactory works 
   * (sometimes, at least not uppercase hacek)
   */
  public void testISOLatin1Folding() throws Exception {
    Reader reader = new StringReader("Česká");
    Tokenizer tokenizer = new WhitespaceTokenizer(reader);
    ISOLatin1AccentFilterFactory factory = new ISOLatin1AccentFilterFactory();
    TokenStream stream = factory.create(tokenizer);
    assertTokenStreamContents(stream, new String[] { "Česka" });
  }
}
