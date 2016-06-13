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
package org.apache.lucene.analysis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Base test class for testing Unicode collation.
 */
public abstract class CollationTestBase extends LuceneTestCase {

  protected String firstRangeBeginningOriginal = "\u062F";
  protected String firstRangeEndOriginal = "\u0698";
  
  protected String secondRangeBeginningOriginal = "\u0633";
  protected String secondRangeEndOriginal = "\u0638";
  
  public void testFarsiRangeFilterCollating(Analyzer analyzer, BytesRef firstBeg, 
                                            BytesRef firstEnd, BytesRef secondBeg,
                                            BytesRef secondEnd) throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(analyzer));
    Document doc = new Document();
    doc.add(new TextField("content", "\u0633\u0627\u0628", Field.Store.YES));
    doc.add(new StringField("body", "body", Field.Store.YES));
    writer.addDocument(doc);
    writer.close();
    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);
    Query query = new TermQuery(new Term("body","body"));

    // Unicode order would include U+0633 in [ U+062F - U+0698 ], but Farsi
    // orders the U+0698 character before the U+0633 character, so the single
    // index Term below should NOT be returned by a TermRangeFilter with a Farsi
    // Collator (or an Arabic one for the case when Farsi searcher not
    // supported).
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(query, Occur.MUST);
    bq.add(new TermRangeQuery("content", firstBeg, firstEnd, true, true), Occur.FILTER);
    ScoreDoc[] result = searcher.search(bq.build(), 1).scoreDocs;
    assertEquals("The index Term should not be included.", 0, result.length);

    bq = new BooleanQuery.Builder();
    bq.add(query, Occur.MUST);
    bq.add(new TermRangeQuery("content", secondBeg, secondEnd, true, true), Occur.FILTER);
    result = searcher.search(bq.build(), 1).scoreDocs;
    assertEquals("The index Term should be included.", 1, result.length);

    reader.close();
    dir.close();
  }
 
  public void testFarsiRangeQueryCollating(Analyzer analyzer, BytesRef firstBeg, 
                                            BytesRef firstEnd, BytesRef secondBeg,
                                            BytesRef secondEnd) throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(analyzer));
    Document doc = new Document();

    // Unicode order would include U+0633 in [ U+062F - U+0698 ], but Farsi
    // orders the U+0698 character before the U+0633 character, so the single
    // index Term below should NOT be returned by a TermRangeQuery with a Farsi
    // Collator (or an Arabic one for the case when Farsi is not supported).
    doc.add(new TextField("content", "\u0633\u0627\u0628", Field.Store.YES));
    writer.addDocument(doc);
    writer.close();
    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);

    Query query = new TermRangeQuery("content", firstBeg, firstEnd, true, true);
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("The index Term should not be included.", 0, hits.length);

    query = new TermRangeQuery("content", secondBeg, secondEnd, true, true);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("The index Term should be included.", 1, hits.length);
    reader.close();
    dir.close();
  }

  public void testFarsiTermRangeQuery(Analyzer analyzer, BytesRef firstBeg,
      BytesRef firstEnd, BytesRef secondBeg, BytesRef secondEnd) throws Exception {

    Directory farsiIndex = newDirectory();
    IndexWriter writer = new IndexWriter(farsiIndex, new IndexWriterConfig(analyzer));
    Document doc = new Document();
    doc.add(new TextField("content", "\u0633\u0627\u0628", Field.Store.YES));
    doc.add(new StringField("body", "body", Field.Store.YES));
    writer.addDocument(doc);
    writer.close();

    IndexReader reader = DirectoryReader.open(farsiIndex);
    IndexSearcher search = newSearcher(reader);
        
    // Unicode order would include U+0633 in [ U+062F - U+0698 ], but Farsi
    // orders the U+0698 character before the U+0633 character, so the single
    // index Term below should NOT be returned by a TermRangeQuery
    // with a Farsi Collator (or an Arabic one for the case when Farsi is 
    // not supported).
    Query csrq 
      = new TermRangeQuery("content", firstBeg, firstEnd, true, true);
    ScoreDoc[] result = search.search(csrq, 1000).scoreDocs;
    assertEquals("The index Term should not be included.", 0, result.length);

    csrq = new TermRangeQuery
      ("content", secondBeg, secondEnd, true, true);
    result = search.search(csrq, 1000).scoreDocs;
    assertEquals("The index Term should be included.", 1, result.length);
    reader.close();
    farsiIndex.close();
  }
  
  // Make sure the documents returned by the search match the expected list
  // Copied from TestSort.java
  private void assertMatches(IndexSearcher searcher, Query query, Sort sort, 
                             String expectedResult) throws IOException {
    ScoreDoc[] result = searcher.search(query, 1000, sort).scoreDocs;
    StringBuilder buff = new StringBuilder(10);
    int n = result.length;
    for (int i = 0 ; i < n ; ++i) {
      Document doc = searcher.doc(result[i].doc);
      IndexableField[] v = doc.getFields("tracer");
      for (int j = 0 ; j < v.length ; ++j) {
        buff.append(v[j].stringValue());
      }
    }
    assertEquals(expectedResult, buff.toString());
  }

  public void assertThreadSafe(final Analyzer analyzer) throws Exception {
    int numTestPoints = 100;
    int numThreads = TestUtil.nextInt(random(), 3, 5);
    final HashMap<String,BytesRef> map = new HashMap<>();
    
    // create a map<String,SortKey> up front.
    // then with multiple threads, generate sort keys for all the keys in the map
    // and ensure they are the same as the ones we produced in serial fashion.

    for (int i = 0; i < numTestPoints; i++) {
      String term = TestUtil.randomSimpleString(random());
      try (TokenStream ts = analyzer.tokenStream("fake", term)) {
        TermToBytesRefAttribute termAtt = ts.addAttribute(TermToBytesRefAttribute.class);
        ts.reset();
        assertTrue(ts.incrementToken());
        // ensure we make a copy of the actual bytes too
        map.put(term, BytesRef.deepCopyOf(termAtt.getBytesRef()));
        assertFalse(ts.incrementToken());
        ts.end();
      }
    }
    
    Thread threads[] = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      threads[i] = new Thread() {
        @Override
        public void run() {
          try {
            for (Map.Entry<String,BytesRef> mapping : map.entrySet()) {
              String term = mapping.getKey();
              BytesRef expected = mapping.getValue();
              try (TokenStream ts = analyzer.tokenStream("fake", term)) {
                TermToBytesRefAttribute termAtt = ts.addAttribute(TermToBytesRefAttribute.class);
                ts.reset();
                assertTrue(ts.incrementToken());
                assertEquals(expected, termAtt.getBytesRef());
                assertFalse(ts.incrementToken());
                ts.end();
              }
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      };
    }
    for (int i = 0; i < numThreads; i++) {
      threads[i].start();
    }
    for (int i = 0; i < numThreads; i++) {
      threads[i].join();
    }
  }
}
