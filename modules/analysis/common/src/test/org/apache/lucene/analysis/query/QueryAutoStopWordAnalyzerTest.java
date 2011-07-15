package org.apache.lucene.analysis.query;
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;

public class QueryAutoStopWordAnalyzerTest extends BaseTokenStreamTestCase {
  String variedFieldValues[] = {"the", "quick", "brown", "fox", "jumped", "over", "the", "lazy", "boring", "dog"};
  String repetitiveFieldValues[] = {"boring", "boring", "vaguelyboring"};
  RAMDirectory dir;
  Analyzer appAnalyzer;
  IndexReader reader;
  QueryAutoStopWordAnalyzer protectedAnalyzer;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = new RAMDirectory();
    appAnalyzer = new MockAnalyzer(random, MockTokenizer.WHITESPACE, false);
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, appAnalyzer));
    int numDocs = 200;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      String variedFieldValue = variedFieldValues[i % variedFieldValues.length];
      String repetitiveFieldValue = repetitiveFieldValues[i % repetitiveFieldValues.length];
      doc.add(new Field("variedField", variedFieldValue, Field.Store.YES, Field.Index.ANALYZED));
      doc.add(new Field("repetitiveField", repetitiveFieldValue, Field.Store.YES, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }
    writer.close();
    reader = IndexReader.open(dir, true);
    protectedAnalyzer = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, appAnalyzer);
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    super.tearDown();
  }

  public void testUninitializedAnalyzer() throws Exception {
    // Note: no calls to "addStopWord"
    // query = "variedField:quick repetitiveField:boring";
    TokenStream protectedTokenStream = protectedAnalyzer.reusableTokenStream("variedField", new StringReader("quick"));
    assertTokenStreamContents(protectedTokenStream, new String[]{"quick"});

    protectedTokenStream = protectedAnalyzer.reusableTokenStream("repetitiveField", new StringReader("boring"));
    assertTokenStreamContents(protectedTokenStream, new String[]{"boring"});
  }

  /*
    * Test method for 'org.apache.lucene.analysis.QueryAutoStopWordAnalyzer.addStopWords(IndexReader)'
    */
  public void testDefaultAddStopWordsIndexReader() throws Exception {
    protectedAnalyzer.addStopWords(reader);
    TokenStream protectedTokenStream = protectedAnalyzer.reusableTokenStream("repetitiveField", new StringReader("boring"));

    assertTokenStreamContents(protectedTokenStream, new String[0]); // Default stop word filtering will remove boring
  }

  /*
    * Test method for 'org.apache.lucene.analysis.QueryAutoStopWordAnalyzer.addStopWords(IndexReader, int)'
    */
  public void testAddStopWordsIndexReaderInt() throws Exception {
    protectedAnalyzer.addStopWords(reader, 1f / 2f);

    TokenStream protectedTokenStream = protectedAnalyzer.reusableTokenStream("repetitiveField", new StringReader("boring"));
    // A filter on terms in > one half of docs remove boring
    assertTokenStreamContents(protectedTokenStream, new String[0]);

    protectedTokenStream = protectedAnalyzer.reusableTokenStream("repetitiveField", new StringReader("vaguelyboring"));
     // A filter on terms in > half of docs should not remove vaguelyBoring
    assertTokenStreamContents(protectedTokenStream, new String[]{"vaguelyboring"});

    protectedAnalyzer.addStopWords(reader, 1f / 4f);
    protectedTokenStream = protectedAnalyzer.reusableTokenStream("repetitiveField", new StringReader("vaguelyboring"));
     // A filter on terms in > quarter of docs should remove vaguelyBoring
    assertTokenStreamContents(protectedTokenStream, new String[0]);
  }

  public void testAddStopWordsIndexReaderStringFloat() throws Exception {
    protectedAnalyzer.addStopWords(reader, "variedField", 1f / 2f);
    TokenStream protectedTokenStream = protectedAnalyzer.reusableTokenStream("repetitiveField", new StringReader("boring"));
    // A filter on one Field should not affect queries on another
    assertTokenStreamContents(protectedTokenStream, new String[]{"boring"});

    protectedAnalyzer.addStopWords(reader, "repetitiveField", 1f / 2f);
    protectedTokenStream = protectedAnalyzer.reusableTokenStream("repetitiveField", new StringReader("boring"));
    // A filter on the right Field should affect queries on it
    assertTokenStreamContents(protectedTokenStream, new String[0]);
  }

  public void testAddStopWordsIndexReaderStringInt() throws Exception {
    int numStopWords = protectedAnalyzer.addStopWords(reader, "repetitiveField", 10);
    assertTrue("Should have identified stop words", numStopWords > 0);

    Term[] t = protectedAnalyzer.getStopWords();
    assertEquals("num terms should = num stopwords returned", t.length, numStopWords);

    int numNewStopWords = protectedAnalyzer.addStopWords(reader, "variedField", 10);
    assertTrue("Should have identified more stop words", numNewStopWords > 0);
    t = protectedAnalyzer.getStopWords();
    assertEquals("num terms should = num stopwords returned", t.length, numStopWords + numNewStopWords);
  }

  public void testNoFieldNamePollution() throws Exception {
    protectedAnalyzer.addStopWords(reader, "repetitiveField", 10);

    TokenStream protectedTokenStream = protectedAnalyzer.reusableTokenStream("repetitiveField", new StringReader("boring"));
    // Check filter set up OK
    assertTokenStreamContents(protectedTokenStream, new String[0]);

    protectedTokenStream = protectedAnalyzer.reusableTokenStream("variedField", new StringReader("boring"));
    // Filter should not prevent stopwords in one field being used in another
    assertTokenStreamContents(protectedTokenStream, new String[]{"boring"});
  }
  
  /*
   * analyzer that does not support reuse
   * it is LetterTokenizer on odd invocations, WhitespaceTokenizer on even.
   */
  private class NonreusableAnalyzer extends Analyzer {
    int invocationCount = 0;
    @Override
    public TokenStream tokenStream(String fieldName, Reader reader) {
      if (++invocationCount % 2 == 0)
        return new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
      else
        return new MockTokenizer(reader, MockTokenizer.SIMPLE, false);
    }
  }
  
  public void testWrappingNonReusableAnalyzer() throws Exception {
    QueryAutoStopWordAnalyzer a = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, new NonreusableAnalyzer());
    a.addStopWords(reader, 10);

    TokenStream tokenStream = a.reusableTokenStream("repetitiveField", new StringReader("boring"));
    assertTokenStreamContents(tokenStream, new String[0]);

    tokenStream = a.reusableTokenStream("repetitiveField", new StringReader("vaguelyboring"));
    assertTokenStreamContents(tokenStream, new String[0]);
  }
  
  public void testTokenStream() throws Exception {
    QueryAutoStopWordAnalyzer a = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false));
    a.addStopWords(reader, 10);
    TokenStream ts = a.tokenStream("repetitiveField", new StringReader("this boring"));
    assertTokenStreamContents(ts, new String[] { "this" });
  }
}
