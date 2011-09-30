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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;

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
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
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
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    super.tearDown();
  }

  //Helper method to query
  private int search(Analyzer a, String queryString) throws IOException, ParseException {
    QueryParser qp = new QueryParser(TEST_VERSION_CURRENT, "repetitiveField", a);
    Query q = qp.parse(queryString);
    IndexSearcher searcher = newSearcher(reader);
    int hits = searcher.search(q, null, 1000).totalHits;
    searcher.close();
    return hits;
  }

  public void testNoStopwords() throws Exception {
    // Note: an empty list of fields passed in
    protectedAnalyzer = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, appAnalyzer, reader, Collections.<String>emptyList(), 1);
    String query = "variedField:quick repetitiveField:boring";
    int numHits1 = search(protectedAnalyzer, query);
    int numHits2 = search(appAnalyzer, query);
    assertEquals("No filtering test", numHits1, numHits2);
  }

  public void testDefaultStopwordsAllFields() throws Exception {
    protectedAnalyzer = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, appAnalyzer, reader);
    int numHits = search(protectedAnalyzer, "repetitiveField:boring");
    assertEquals("Default filter should remove all docs", 0, numHits);
  }

  public void testStopwordsAllFieldsMaxPercentDocs() throws Exception {
    protectedAnalyzer = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, appAnalyzer, reader, 1f / 2f);
    int numHits = search(protectedAnalyzer, "repetitiveField:boring");
    assertEquals("A filter on terms in > one half of docs remove boring docs", 0, numHits);

    numHits = search(protectedAnalyzer, "repetitiveField:vaguelyboring");
    assertTrue("A filter on terms in > half of docs should not remove vaguelyBoring docs", numHits > 1);

    protectedAnalyzer = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, appAnalyzer, reader, 1f / 4f);
    numHits = search(protectedAnalyzer, "repetitiveField:vaguelyboring");
    assertEquals("A filter on terms in > quarter of docs should remove vaguelyBoring docs", 0, numHits);
  }

  public void testStopwordsPerFieldMaxPercentDocs() throws Exception {
    protectedAnalyzer = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, appAnalyzer, reader, Arrays.asList("variedField"), 1f / 2f);
    int numHits = search(protectedAnalyzer, "repetitiveField:boring");
    assertTrue("A filter on one Field should not affect queris on another", numHits > 0);

    protectedAnalyzer = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, appAnalyzer, reader, Arrays.asList("variedField", "repetitiveField"), 1f / 2f);
    numHits = search(protectedAnalyzer, "repetitiveField:boring");
    assertEquals("A filter on the right Field should affect queries on it", numHits, 0);
  }

  public void testStopwordsPerFieldMaxDocFreq() throws Exception {
    protectedAnalyzer = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, appAnalyzer, reader, Arrays.asList("repetitiveField"), 10);
    int numStopWords = protectedAnalyzer.getStopWords("repetitiveField").length;
    assertTrue("Should have identified stop words", numStopWords > 0);

    protectedAnalyzer = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, appAnalyzer, reader, Arrays.asList("repetitiveField", "variedField"), 10);
    int numNewStopWords = protectedAnalyzer.getStopWords("repetitiveField").length + protectedAnalyzer.getStopWords("variedField").length;
    assertTrue("Should have identified more stop words", numNewStopWords > numStopWords);
  }

  public void testNoFieldNamePollution() throws Exception {
    protectedAnalyzer = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, appAnalyzer, reader, Arrays.asList("repetitiveField"), 10);
    int numHits = search(protectedAnalyzer, "repetitiveField:boring");
    assertEquals("Check filter set up OK", 0, numHits);

    numHits = search(protectedAnalyzer, "variedField:boring");
    assertTrue("Filter should not prevent stopwords in one field being used in another ", numHits > 0);
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
    QueryAutoStopWordAnalyzer a = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, new NonreusableAnalyzer(), reader, 10);
    int numHits = search(a, "repetitiveField:boring");
    assertTrue(numHits == 0);
    numHits = search(a, "repetitiveField:vaguelyboring");
    assertTrue(numHits == 0);
  }
  
  public void testTokenStream() throws Exception {
    QueryAutoStopWordAnalyzer a = new QueryAutoStopWordAnalyzer(
        TEST_VERSION_CURRENT,
        new MockAnalyzer(random, MockTokenizer.WHITESPACE, false), reader, 10);
    TokenStream ts = a.tokenStream("repetitiveField", new StringReader("this boring"));
    assertTokenStreamContents(ts, new String[] { "this" });
  }
}
