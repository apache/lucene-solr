package org.apache.lucene.collation;

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
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeFilter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Document;
import org.apache.lucene.util.IndexableBinaryStringTools;
import org.apache.lucene.queryParser.analyzing.AnalyzingQueryParser;

import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.ByteBuffer;


public class CollationTestBase extends TestCase {

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
   */
  protected String encodeCollationKey(byte[] keyBits) {
    ByteBuffer begBuf = ByteBuffer.wrap(keyBits);
    // Ensure that the backing char[] array is large enough to hold the encoded
    // Binary String
    char[] encodedBegArray 
      = new char[IndexableBinaryStringTools.getEncodedLength(begBuf)];
    CharBuffer encodedBegBuf = CharBuffer.wrap(encodedBegArray); 
    IndexableBinaryStringTools.encode(begBuf, encodedBegBuf);
    return new String(encodedBegArray);
  }
  
  public void testFarsiQueryParserCollating(Analyzer analyzer) throws Exception {

    RAMDirectory ramDir = new RAMDirectory();
    IndexWriter writer = new IndexWriter
      (ramDir, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
    Document doc = new Document();
    doc.add(new Field("content", "\u0633\u0627\u0628", 
                      Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);
    writer.close();
    IndexSearcher is = new IndexSearcher(ramDir, true);

    AnalyzingQueryParser aqp = new AnalyzingQueryParser("content", analyzer);
    aqp.setLowercaseExpandedTerms(false);

    // Unicode order would include U+0633 in [ U+062F - U+0698 ], but Farsi
    // orders the U+0698 character before the U+0633 character, so the single
    // index Term below should NOT be returned by a TermRangeQuery
    // with a Farsi Collator (or an Arabic one for the case when Farsi is not
    // supported).
      
    // Test TermRangeQuery
    ScoreDoc[] result
      = is.search(aqp.parse("[ \u062F TO \u0698 ]"), null, 1000).scoreDocs;
    assertEquals("The index Term should not be included.", 0, result.length);

    result = is.search(aqp.parse("[ \u0633 TO \u0638 ]"), null, 1000).scoreDocs;
    assertEquals("The index Term should be included.", 1, result.length);

    is.close();
  }
  
  
  public void testFarsiRangeFilterCollating(Analyzer analyzer, String firstBeg, 
                                            String firstEnd, String secondBeg,
                                            String secondEnd) throws Exception {
    RAMDirectory ramDir = new RAMDirectory();
    IndexWriter writer = new IndexWriter
      (ramDir, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
    Document doc = new Document();
    doc.add(new Field("content", "\u0633\u0627\u0628", 
                      Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("body", "body",
                      Field.Store.YES, Field.Index.NOT_ANALYZED));
    writer.addDocument(doc);
    writer.close();
    IndexSearcher searcher = new IndexSearcher(ramDir, true);
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

    searcher.close();
  }
 
  public void testFarsiRangeQueryCollating(Analyzer analyzer, String firstBeg, 
                                            String firstEnd, String secondBeg,
                                            String secondEnd) throws Exception {
    RAMDirectory ramDir = new RAMDirectory();
    IndexWriter writer = new IndexWriter
      (ramDir, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
    Document doc = new Document();

    // Unicode order would include U+0633 in [ U+062F - U+0698 ], but Farsi
    // orders the U+0698 character before the U+0633 character, so the single
    // index Term below should NOT be returned by a TermRangeQuery with a Farsi
    // Collator (or an Arabic one for the case when Farsi is not supported).
    doc.add(new Field("content", "\u0633\u0627\u0628", 
                      Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);
    writer.close();
    IndexSearcher searcher = new IndexSearcher(ramDir, true);

    Query query = new TermRangeQuery("content", firstBeg, firstEnd, true, true);
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("The index Term should not be included.", 0, hits.length);

    query = new TermRangeQuery("content", secondBeg, secondEnd, true, true);
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("The index Term should be included.", 1, hits.length);
    searcher.close();
  }

  public void testFarsiTermRangeQuery
    (Analyzer analyzer, String firstBeg, String firstEnd, 
     String secondBeg, String secondEnd) throws Exception {

    RAMDirectory farsiIndex = new RAMDirectory();
    IndexWriter writer = new IndexWriter
      (farsiIndex, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
    Document doc = new Document();
    doc.add(new Field("content", "\u0633\u0627\u0628", 
                      Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("body", "body",
                      Field.Store.YES, Field.Index.NOT_ANALYZED));
    writer.addDocument(doc);
    writer.close();

    IndexReader reader = IndexReader.open(farsiIndex, true);
    IndexSearcher search = new IndexSearcher(reader);
        
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
    search.close();
  }
  
  // Test using various international locales with accented characters (which
  // sort differently depending on locale)
  //
  // Copied (and slightly modified) from 
  // org.apache.lucene.search.TestSort.testInternationalSort()
  //  
  public void testCollationKeySort(Analyzer usAnalyzer,
                                   Analyzer franceAnalyzer,
                                   Analyzer swedenAnalyzer,
                                   Analyzer denmarkAnalyzer,
                                   String usResult) throws Exception {
    RAMDirectory indexStore = new RAMDirectory();
    PerFieldAnalyzerWrapper analyzer
      = new PerFieldAnalyzerWrapper(new WhitespaceAnalyzer());
    analyzer.addAnalyzer("US", usAnalyzer);
    analyzer.addAnalyzer("France", franceAnalyzer);
    analyzer.addAnalyzer("Sweden", swedenAnalyzer);
    analyzer.addAnalyzer("Denmark", denmarkAnalyzer);
    IndexWriter writer = new IndexWriter 
      (indexStore, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);

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

    for (int i = 0 ; i < sortData.length ; ++i) {
      Document doc = new Document();
      doc.add(new Field("tracer", sortData[i][0], 
                        Field.Store.YES, Field.Index.NO));
      doc.add(new Field("contents", sortData[i][1], 
                        Field.Store.NO, Field.Index.ANALYZED));
      if (sortData[i][2] != null) 
        doc.add(new Field("US", sortData[i][2], 
                          Field.Store.NO, Field.Index.ANALYZED));
      if (sortData[i][3] != null) 
        doc.add(new Field("France", sortData[i][3], 
                          Field.Store.NO, Field.Index.ANALYZED));
      if (sortData[i][4] != null)
        doc.add(new Field("Sweden", sortData[i][4], 
                          Field.Store.NO, Field.Index.ANALYZED));
      if (sortData[i][5] != null) 
        doc.add(new Field("Denmark", sortData[i][5], 
                          Field.Store.NO, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }
    writer.optimize();
    writer.close();
    Searcher searcher = new IndexSearcher(indexStore, true);

    Sort sort = new Sort();
    Query queryX = new TermQuery(new Term ("contents", "x"));
    Query queryY = new TermQuery(new Term ("contents", "y"));
    
    sort.setSort(new SortField("US", SortField.STRING));
    assertMatches(searcher, queryY, sort, usResult);

    sort.setSort(new SortField("France", SortField.STRING));
    assertMatches(searcher, queryX, sort, "EACGI");

    sort.setSort(new SortField("Sweden", SortField.STRING));
    assertMatches(searcher, queryY, sort, "BJDFH");

    sort.setSort(new SortField("Denmark", SortField.STRING));
    assertMatches(searcher, queryY, sort, "BJDHF");
  }
    
  // Make sure the documents returned by the search match the expected list
  // Copied from TestSort.java
  private void assertMatches(Searcher searcher, Query query, Sort sort, 
                             String expectedResult) throws IOException {
    ScoreDoc[] result = searcher.search(query, null, 1000, sort).scoreDocs;
    StringBuilder buff = new StringBuilder(10);
    int n = result.length;
    for (int i = 0 ; i < n ; ++i) {
      Document doc = searcher.doc(result[i].doc);
      String[] v = doc.getValues("tracer");
      for (int j = 0 ; j < v.length ; ++j) {
        buff.append(v[j]);
      }
    }
    assertEquals(expectedResult, buff.toString());
  }
}
