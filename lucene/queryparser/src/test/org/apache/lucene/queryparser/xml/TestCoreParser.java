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
package org.apache.lucene.queryparser.xml;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;


public class TestCoreParser extends LuceneTestCase {

  final private static String defaultField = "contents";
  private static Analyzer analyzer;
  private static CoreParser coreParser;
  private static Directory dir;
  private static IndexReader reader;
  protected static IndexSearcher searcher;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // TODO: rewrite test (this needs to set QueryParser.enablePositionIncrements, too, for work with CURRENT):
    analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true, MockTokenFilter.ENGLISH_STOPSET);
    //initialize the parser
    coreParser = new CoreParser(defaultField, analyzer);

    BufferedReader d = new BufferedReader(new InputStreamReader(
        TestCoreParser.class.getResourceAsStream("reuters21578.txt"), StandardCharsets.US_ASCII));
    dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(analyzer));
    String line = d.readLine();
    while (line != null) {
      int endOfDate = line.indexOf('\t');
      String date = line.substring(0, endOfDate).trim();
      String content = line.substring(endOfDate).trim();
      Document doc = new Document();
      doc.add(newTextField("date", date, Field.Store.YES));
      doc.add(newTextField("contents", content, Field.Store.YES));
      doc.add(new IntField("date2", Integer.valueOf(date), Field.Store.NO));
      writer.addDocument(doc);
      line = d.readLine();
    }
    d.close();
    writer.close();
    reader = DirectoryReader.open(dir);
    searcher = newSearcher(reader);

  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    dir.close();
    reader = null;
    searcher = null;
    dir = null;
    coreParser = null;
    analyzer = null;
  }

  public void testSimpleXML() throws ParserException, IOException {
    Query q = parse("TermQuery.xml");
    dumpResults("TermQuery", q, 5);
  }

  public void testSimpleTermsQueryXML() throws ParserException, IOException {
    Query q = parse("TermsQuery.xml");
    dumpResults("TermsQuery", q, 5);
  }

  public void testBooleanQueryXML() throws ParserException, IOException {
    Query q = parse("BooleanQuery.xml");
    dumpResults("BooleanQuery", q, 5);
  }
  
  public void testDisjunctionMaxQueryXML() throws ParserException, IOException {
    Query q = parse("DisjunctionMaxQuery.xml");
    assertTrue(q instanceof DisjunctionMaxQuery);
    DisjunctionMaxQuery d = (DisjunctionMaxQuery)q;
    assertEquals(0.0f, d.getTieBreakerMultiplier(), 0.0001f);
    assertEquals(2, d.getDisjuncts().size());
    DisjunctionMaxQuery ndq = (DisjunctionMaxQuery) d.getDisjuncts().get(1);
    assertEquals(1.2f, ndq.getTieBreakerMultiplier(), 0.0001f);
    assertEquals(1, ndq.getDisjuncts().size());
  }

  public void testRangeQueryXML() throws ParserException, IOException {
    Query q = parse("RangeQuery.xml");
    dumpResults("RangeQuery", q, 5);
  }

  public void testRangeFilterQueryXML() throws ParserException, IOException {
    Query q = parse("RangeFilterQuery.xml");
    dumpResults("RangeFilter", q, 5);
  }

  public void testUserQueryXML() throws ParserException, IOException {
    Query q = parse("UserInputQuery.xml");
    dumpResults("UserInput with Filter", q, 5);
  }

  public void testCustomFieldUserQueryXML() throws ParserException, IOException {
    Query q = parse("UserInputQueryCustomField.xml");
    int h = searcher.search(q, 1000).totalHits;
    assertEquals("UserInputQueryCustomField should produce 0 result ", 0, h);
  }

  public void testBoostingTermQueryXML() throws Exception {
    Query q = parse("BoostingTermQuery.xml");
    dumpResults("BoostingTermQuery", q, 5);
  }

  public void testSpanTermXML() throws Exception {
    Query q = parse("SpanQuery.xml");
    dumpResults("Span Query", q, 5);
  }

  public void testConstantScoreQueryXML() throws Exception {
    Query q = parse("ConstantScoreQuery.xml");
    dumpResults("ConstantScoreQuery", q, 5);
  }

  public void testMatchAllDocsPlusFilterXML() throws ParserException, IOException {
    Query q = parse("MatchAllDocsQuery.xml");
    dumpResults("MatchAllDocsQuery with range filter", q, 5);
  }

  public void testNestedBooleanQuery() throws ParserException, IOException {
    Query q = parse("NestedBooleanQuery.xml");
    dumpResults("Nested Boolean query", q, 5);
  }

  public void testCachedFilterXML() throws ParserException, IOException {
    Query q = parse("CachedFilter.xml");
    dumpResults("Cached filter", q, 5);
  }

  public void testNumericRangeFilterQueryXML() throws ParserException, IOException {
    Query q = parse("NumericRangeFilterQuery.xml");
    dumpResults("NumericRangeFilter", q, 5);
  }

  public void testNumericRangeQueryXML() throws ParserException, IOException {
    Query q = parse("NumericRangeQuery.xml");
    dumpResults("NumericRangeQuery", q, 5);
  }

  //================= Helper methods ===================================

  protected String defaultField() {
    return defaultField;
  }

  protected Analyzer analyzer() {
    return analyzer;
  }

  protected CoreParser coreParser() {
    return coreParser;
  }

  protected Query parse(String xmlFileName) throws ParserException, IOException {
    InputStream xmlStream = TestCoreParser.class.getResourceAsStream(xmlFileName);
    Query result = coreParser().parse(xmlStream);
    xmlStream.close();
    return result;
  }

  protected Query rewrite(Query q) throws IOException {
    return q.rewrite(reader);
  }

  protected void dumpResults(String qType, Query q, int numDocs) throws IOException {
    if (VERBOSE) {
      System.out.println("TEST: query=" + q);
    }
    TopDocs hits = searcher.search(q, numDocs);
    assertTrue(qType + " should produce results ", hits.totalHits > 0);
    if (VERBOSE) {
      System.out.println("=========" + qType + "============");
      ScoreDoc[] scoreDocs = hits.scoreDocs;
      for (int i = 0; i < Math.min(numDocs, hits.totalHits); i++) {
        Document ldoc = searcher.doc(scoreDocs[i].doc);
        System.out.println("[" + ldoc.get("date") + "]" + ldoc.get("contents"));
      }
      System.out.println();
    }
  }
}
