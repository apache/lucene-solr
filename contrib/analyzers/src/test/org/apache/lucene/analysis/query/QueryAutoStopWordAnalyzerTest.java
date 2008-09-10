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

import junit.framework.TestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;

public class QueryAutoStopWordAnalyzerTest extends TestCase {
  String variedFieldValues[] = {"the", "quick", "brown", "fox", "jumped", "over", "the", "lazy", "boring", "dog"};
  String repetitiveFieldValues[] = {"boring", "boring", "vaguelyboring"};
  RAMDirectory dir;
  Analyzer appAnalyzer;
  IndexReader reader;
  QueryAutoStopWordAnalyzer protectedAnalyzer;

  protected void setUp() throws Exception {
    super.setUp();
    dir = new RAMDirectory();
    appAnalyzer = new WhitespaceAnalyzer();
    IndexWriter writer = new IndexWriter(dir, appAnalyzer, true, IndexWriter.MaxFieldLength.UNLIMITED);
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
    reader = IndexReader.open(dir);
    protectedAnalyzer = new QueryAutoStopWordAnalyzer(appAnalyzer);
  }

  protected void tearDown() throws Exception {
    super.tearDown();
    reader.close();
  }

  //Helper method to query
  private Hits search(Analyzer a, String queryString) throws IOException, ParseException {
    QueryParser qp = new QueryParser("repetitiveField", a);
    Query q = qp.parse(queryString);
    return new IndexSearcher(reader).search(q);
  }

  public void testUninitializedAnalyzer() throws Exception {
    //Note: no calls to "addStopWord"
    String query = "variedField:quick repetitiveField:boring";
    Hits h = search(protectedAnalyzer, query);
    Hits h2 = search(appAnalyzer, query);
    assertEquals("No filtering test", h.length(), h2.length());
  }

  /*
    * Test method for 'org.apache.lucene.analysis.QueryAutoStopWordAnalyzer.addStopWords(IndexReader)'
    */
  public void testDefaultAddStopWordsIndexReader() throws Exception {
    protectedAnalyzer.addStopWords(reader);
    Hits h = search(protectedAnalyzer, "repetitiveField:boring");
    assertEquals("Default filter should remove all docs", 0, h.length());
  }


  /*
    * Test method for 'org.apache.lucene.analysis.QueryAutoStopWordAnalyzer.addStopWords(IndexReader, int)'
    */
  public void testAddStopWordsIndexReaderInt() throws Exception {
    protectedAnalyzer.addStopWords(reader, 1f / 2f);
    Hits h = search(protectedAnalyzer, "repetitiveField:boring");
    assertEquals("A filter on terms in > one half of docs remove boring docs", 0, h.length());

    h = search(protectedAnalyzer, "repetitiveField:vaguelyboring");
    assertTrue("A filter on terms in > half of docs should not remove vaguelyBoring docs", h.length() > 1);

    protectedAnalyzer.addStopWords(reader, 1f / 4f);
    h = search(protectedAnalyzer, "repetitiveField:vaguelyboring");
    assertEquals("A filter on terms in > quarter of docs should remove vaguelyBoring docs", 0, h.length());
  }


  public void testAddStopWordsIndexReaderStringFloat() throws Exception {
    protectedAnalyzer.addStopWords(reader, "variedField", 1f / 2f);
    Hits h = search(protectedAnalyzer, "repetitiveField:boring");
    assertTrue("A filter on one Field should not affect queris on another", h.length() > 0);

    protectedAnalyzer.addStopWords(reader, "repetitiveField", 1f / 2f);
    h = search(protectedAnalyzer, "repetitiveField:boring");
    assertEquals("A filter on the right Field should affect queries on it", h.length(), 0);
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
    Hits h = search(protectedAnalyzer, "repetitiveField:boring");
    assertEquals("Check filter set up OK", 0, h.length());

    h = search(protectedAnalyzer, "variedField:boring");
    assertTrue("Filter should not prevent stopwords in one field being used in another ", h.length() > 0);

  }


}
