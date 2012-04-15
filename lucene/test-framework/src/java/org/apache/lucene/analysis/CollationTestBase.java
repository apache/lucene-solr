package org.apache.lucene.analysis;

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
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeFilter;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IndexableBinaryStringTools;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Base test class for testing Unicode collation.
 */
public abstract class CollationTestBase extends LuceneTestCase {

  protected String firstRangeBeginningOriginal = "\u062F";
  protected String firstRangeEndOriginal = "\u0698";
  
  protected String secondRangeBeginningOriginal = "\u0633";
  protected String secondRangeEndOriginal = "\u0638";
  
  /**
   * Convenience method to perform the same function as CollationKeyFilter.
   *  
   * @param keyBits the result from 
   *  collator.getCollationKey(original).toByteArray()
   * @return The encoded collation key for the original String
   * @deprecated only for testing deprecated filters
   */
  @Deprecated
  protected String encodeCollationKey(byte[] keyBits) {
    // Ensure that the backing char[] array is large enough to hold the encoded
    // Binary String
    int encodedLength = IndexableBinaryStringTools.getEncodedLength(keyBits, 0, keyBits.length);
    char[] encodedBegArray = new char[encodedLength];
    IndexableBinaryStringTools.encode(keyBits, 0, keyBits.length, encodedBegArray, 0, encodedLength);
    return new String(encodedBegArray);
  }
  
  public void testFarsiRangeFilterCollating(Analyzer analyzer, BytesRef firstBeg, 
                                            BytesRef firstEnd, BytesRef secondBeg,
                                            BytesRef secondEnd) throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(
        TEST_VERSION_CURRENT, analyzer));
    Document doc = new Document();
    doc.add(new Field("content", "\u0633\u0627\u0628", TextField.TYPE_STORED));
    doc.add(new Field("body", "body", StringField.TYPE_STORED));
    writer.addDocument(doc);
    writer.close();
    IndexReader reader = IndexReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);
    Query query = new TermQuery(new Term("body","body"));

    // Unicode order would include U+0633 in [ U+062F - U+0698 ], but Farsi
    // orders the U+0698 character before the U+0633 character, so the single
    // index Term below should NOT be returned by a TermRangeFilter with a Farsi
    // Collator (or an Arabic one for the case when Farsi searcher not
    // supported).
    ScoreDoc[] result = searcher.search
      (query, new TermRangeFilter("content", firstBeg, firstEnd, true, true), 1).scoreDocs;
    assertEquals("The index Term should not be included.", 0, result.length);

    result = searcher.search
      (query, new TermRangeFilter("content", secondBeg, secondEnd, true, true), 1).scoreDocs;
    assertEquals("The index Term should be included.", 1, result.length);

    reader.close();
    dir.close();
  }
 
  public void testFarsiRangeQueryCollating(Analyzer analyzer, BytesRef firstBeg, 
                                            BytesRef firstEnd, BytesRef secondBeg,
                                            BytesRef secondEnd) throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(
        TEST_VERSION_CURRENT, analyzer));
    Document doc = new Document();

    // Unicode order would include U+0633 in [ U+062F - U+0698 ], but Farsi
    // orders the U+0698 character before the U+0633 character, so the single
    // index Term below should NOT be returned by a TermRangeQuery with a Farsi
    // Collator (or an Arabic one for the case when Farsi is not supported).
    doc.add(new Field("content", "\u0633\u0627\u0628", TextField.TYPE_STORED));
    writer.addDocument(doc);
    writer.close();
    IndexReader reader = IndexReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);

    Query query = new TermRangeQuery("content", firstBeg, firstEnd, true, true);
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("The index Term should not be included.", 0, hits.length);

    query = new TermRangeQuery("content", secondBeg, secondEnd, true, true);
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("The index Term should be included.", 1, hits.length);
    reader.close();
    dir.close();
  }

  public void testFarsiTermRangeQuery(Analyzer analyzer, BytesRef firstBeg,
      BytesRef firstEnd, BytesRef secondBeg, BytesRef secondEnd) throws Exception {

    Directory farsiIndex = newDirectory();
    IndexWriter writer = new IndexWriter(farsiIndex, new IndexWriterConfig(
        TEST_VERSION_CURRENT, analyzer));
    Document doc = new Document();
    doc.add(new Field("content", "\u0633\u0627\u0628", TextField.TYPE_STORED));
    doc.add(new Field("body", "body", StringField.TYPE_STORED));
    writer.addDocument(doc);
    writer.close();

    IndexReader reader = IndexReader.open(farsiIndex);
    IndexSearcher search = newSearcher(reader);
        
    // Unicode order would include U+0633 in [ U+062F - U+0698 ], but Farsi
    // orders the U+0698 character before the U+0633 character, so the single
    // index Term below should NOT be returned by a TermRangeQuery
    // with a Farsi Collator (or an Arabic one for the case when Farsi is 
    // not supported).
    Query csrq 
      = new TermRangeQuery("content", firstBeg, firstEnd, true, true);
    ScoreDoc[] result = search.search(csrq, null, 1000).scoreDocs;
    assertEquals("The index Term should not be included.", 0, result.length);

    csrq = new TermRangeQuery
      ("content", secondBeg, secondEnd, true, true);
    result = search.search(csrq, null, 1000).scoreDocs;
    assertEquals("The index Term should be included.", 1, result.length);
    reader.close();
    farsiIndex.close();
  }
  
  // Test using various international locales with accented characters (which
  // sort differently depending on locale)
  //
  // Copied (and slightly modified) from 
  // org.apache.lucene.search.TestSort.testInternationalSort()
  //  
  // TODO: this test is really fragile. there are already 3 different cases,
  // depending upon unicode version.
  public void testCollationKeySort(Analyzer usAnalyzer,
                                   Analyzer franceAnalyzer,
                                   Analyzer swedenAnalyzer,
                                   Analyzer denmarkAnalyzer,
                                   String usResult,
                                   String frResult,
                                   String svResult,
                                   String dkResult) throws Exception {
    Directory indexStore = newDirectory();
    IndexWriter writer = new IndexWriter(indexStore, new IndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));

    // document data:
    // the tracer field is used to determine which document was hit
    String[][] sortData = new String[][] {
      // tracer contents US                 France             Sweden (sv_SE)     Denmark (da_DK)
      {  "A",   "x",     "p\u00EAche",      "p\u00EAche",      "p\u00EAche",      "p\u00EAche"      },
      {  "B",   "y",     "HAT",             "HAT",             "HAT",             "HAT"             },
      {  "C",   "x",     "p\u00E9ch\u00E9", "p\u00E9ch\u00E9", "p\u00E9ch\u00E9", "p\u00E9ch\u00E9" },
      {  "D",   "y",     "HUT",             "HUT",             "HUT",             "HUT"             },
      {  "E",   "x",     "peach",           "peach",           "peach",           "peach"           },
      {  "F",   "y",     "H\u00C5T",        "H\u00C5T",        "H\u00C5T",        "H\u00C5T"        },
      {  "G",   "x",     "sin",             "sin",             "sin",             "sin"             },
      {  "H",   "y",     "H\u00D8T",        "H\u00D8T",        "H\u00D8T",        "H\u00D8T"        },
      {  "I",   "x",     "s\u00EDn",        "s\u00EDn",        "s\u00EDn",        "s\u00EDn"        },
      {  "J",   "y",     "HOT",             "HOT",             "HOT",             "HOT"             },
    };

    FieldType customType = new FieldType();
    customType.setStored(true);
    
    for (int i = 0 ; i < sortData.length ; ++i) {
      Document doc = new Document();
      doc.add(new Field("tracer", sortData[i][0], customType));
      doc.add(new TextField("contents", sortData[i][1]));
      if (sortData[i][2] != null) 
        doc.add(new TextField("US", usAnalyzer.tokenStream("US", new StringReader(sortData[i][2]))));
      if (sortData[i][3] != null) 
        doc.add(new TextField("France", franceAnalyzer.tokenStream("France", new StringReader(sortData[i][3]))));
      if (sortData[i][4] != null)
        doc.add(new TextField("Sweden", swedenAnalyzer.tokenStream("Sweden", new StringReader(sortData[i][4]))));
      if (sortData[i][5] != null) 
        doc.add(new TextField("Denmark", denmarkAnalyzer.tokenStream("Denmark", new StringReader(sortData[i][5]))));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    writer.close();
    IndexReader reader = IndexReader.open(indexStore);
    IndexSearcher searcher = new IndexSearcher(reader);

    Sort sort = new Sort();
    Query queryX = new TermQuery(new Term ("contents", "x"));
    Query queryY = new TermQuery(new Term ("contents", "y"));
    
    sort.setSort(new SortField("US", SortField.Type.STRING));
    assertMatches(searcher, queryY, sort, usResult);

    sort.setSort(new SortField("France", SortField.Type.STRING));
    assertMatches(searcher, queryX, sort, frResult);

    sort.setSort(new SortField("Sweden", SortField.Type.STRING));
    assertMatches(searcher, queryY, sort, svResult);

    sort.setSort(new SortField("Denmark", SortField.Type.STRING));
    assertMatches(searcher, queryY, sort, dkResult);
    reader.close();
    indexStore.close();
  }
    
  // Make sure the documents returned by the search match the expected list
  // Copied from TestSort.java
  private void assertMatches(IndexSearcher searcher, Query query, Sort sort, 
                             String expectedResult) throws IOException {
    ScoreDoc[] result = searcher.search(query, null, 1000, sort).scoreDocs;
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
    int numThreads = _TestUtil.nextInt(random(), 3, 5);
    final HashMap<String,BytesRef> map = new HashMap<String,BytesRef>();
    
    // create a map<String,SortKey> up front.
    // then with multiple threads, generate sort keys for all the keys in the map
    // and ensure they are the same as the ones we produced in serial fashion.

    for (int i = 0; i < numTestPoints; i++) {
      String term = _TestUtil.randomSimpleString(random());
      TokenStream ts = analyzer.tokenStream("fake", new StringReader(term));
      TermToBytesRefAttribute termAtt = ts.addAttribute(TermToBytesRefAttribute.class);
      BytesRef bytes = termAtt.getBytesRef();
      ts.reset();
      assertTrue(ts.incrementToken());
      termAtt.fillBytesRef();
      // ensure we make a copy of the actual bytes too
      map.put(term, BytesRef.deepCopyOf(bytes));
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
              TokenStream ts = analyzer.tokenStream("fake", new StringReader(term));
              TermToBytesRefAttribute termAtt = ts.addAttribute(TermToBytesRefAttribute.class);
              BytesRef bytes = termAtt.getBytesRef();
              ts.reset();
              assertTrue(ts.incrementToken());
              termAtt.fillBytesRef();
              assertEquals(expected, bytes);
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
