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
package org.apache.lucene.queries.mlt;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestMoreLikeThis extends LuceneTestCase {
  private Directory directory;
  private IndexReader reader;
  private IndexSearcher searcher;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    
    // Add series of docs with specific information for MoreLikeThis
    addDoc(writer, "lucene");
    addDoc(writer, "lucene release");
    addDoc(writer, "apache");
    addDoc(writer, "apache lucene");

    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }
  
  private void addDoc(RandomIndexWriter writer, String text) throws IOException {
    Document doc = new Document();
    doc.add(newTextField("text", text, Field.Store.YES));
    writer.addDocument(doc);
  }

  private void addDoc(RandomIndexWriter writer, String[] texts) throws IOException {
    Document doc = new Document();
    for (String text : texts) {
      doc.add(newTextField("text", text, Field.Store.YES));
    }
    writer.addDocument(doc);
  }

  public void testBoostFactor() throws Throwable {
    Map<String,Float> originalValues = getOriginalValues();
    
    MoreLikeThis mlt = new MoreLikeThis(reader);
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    mlt.setAnalyzer(analyzer);
    mlt.setMinDocFreq(1);
    mlt.setMinTermFreq(1);
    mlt.setMinWordLen(1);
    mlt.setFieldNames(new String[] {"text"});
    mlt.setBoost(true);
    
    // this mean that every term boost factor will be multiplied by this
    // number
    float boostFactor = 5;
    mlt.setBoostFactor(boostFactor);
    
    BooleanQuery query = (BooleanQuery) mlt.like("text", new StringReader(
        "lucene release"));
    Collection<BooleanClause> clauses = query.clauses();
    
    assertEquals("Expected " + originalValues.size() + " clauses.",
        originalValues.size(), clauses.size());

    for (BooleanClause clause : clauses) {
      BoostQuery bq = (BoostQuery) clause.getQuery();
      TermQuery tq = (TermQuery) bq.getQuery();
      Float termBoost = originalValues.get(tq.getTerm().text());
      assertNotNull("Expected term " + tq.getTerm().text(), termBoost);

      float totalBoost = termBoost * boostFactor;
      assertEquals("Expected boost of " + totalBoost + " for term '"
          + tq.getTerm().text() + "' got " + bq.getBoost(), totalBoost, bq
          .getBoost(), 0.0001);
    }
    analyzer.close();
  }
  
  private Map<String,Float> getOriginalValues() throws IOException {
    Map<String,Float> originalValues = new HashMap<>();
    MoreLikeThis mlt = new MoreLikeThis(reader);
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    mlt.setAnalyzer(analyzer);
    mlt.setMinDocFreq(1);
    mlt.setMinTermFreq(1);
    mlt.setMinWordLen(1);
    mlt.setFieldNames(new String[] {"text"});
    mlt.setBoost(true);
    BooleanQuery query = (BooleanQuery) mlt.like("text", new StringReader(
        "lucene release"));
    Collection<BooleanClause> clauses = query.clauses();

    for (BooleanClause clause : clauses) {
      BoostQuery bq = (BoostQuery) clause.getQuery();
      TermQuery tq = (TermQuery) bq.getQuery();
      originalValues.put(tq.getTerm().text(), bq.getBoost());
    }
    analyzer.close();
    return originalValues;
  }
  
  // LUCENE-3326
  public void testMultiFields() throws Exception {
    MoreLikeThis mlt = new MoreLikeThis(reader);
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    mlt.setAnalyzer(analyzer);
    mlt.setMinDocFreq(1);
    mlt.setMinTermFreq(1);
    mlt.setMinWordLen(1);
    mlt.setFieldNames(new String[] {"text", "foobar"});
    mlt.like("foobar", new StringReader("this is a test"));
    analyzer.close();
  }

  // LUCENE-5725
  public void testMultiValues() throws Exception {
    MoreLikeThis mlt = new MoreLikeThis(reader);
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.KEYWORD, false);
    mlt.setAnalyzer(analyzer);
    mlt.setMinDocFreq(1);
    mlt.setMinTermFreq(1);
    mlt.setMinWordLen(1);
    mlt.setFieldNames(new String[] {"text"});

    BooleanQuery query = (BooleanQuery) mlt.like("text",
        new StringReader("lucene"), new StringReader("lucene release"),
        new StringReader("apache"), new StringReader("apache lucene"));
    Collection<BooleanClause> clauses = query.clauses();
    assertEquals("Expected 2 clauses only!", 2, clauses.size());
    for (BooleanClause clause : clauses) {
      Term term = ((TermQuery) clause.getQuery()).getTerm();
      assertTrue(Arrays.asList(new Term("text", "lucene"), new Term("text", "apache")).contains(term));
    }
    analyzer.close();
  }

  // just basic equals/hashcode etc
  public void testMoreLikeThisQuery() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    Query query = new MoreLikeThisQuery("this is a test", new String[] { "text" }, analyzer, "text");
    QueryUtils.check(random(), query, searcher);
    analyzer.close();
  }

  public void testTopN() throws Exception {
    int numDocs = 100;
    int topN = 25;

    // add series of docs with terms of decreasing df
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < numDocs; i++) {
      addDoc(writer, generateStrSeq(0, i + 1));
    }
    IndexReader reader = writer.getReader();
    writer.close();

    // setup MLT query
    MoreLikeThis mlt = new MoreLikeThis(reader);
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    mlt.setAnalyzer(analyzer);
    mlt.setMaxQueryTerms(topN);
    mlt.setMinDocFreq(1);
    mlt.setMinTermFreq(1);
    mlt.setMinWordLen(1);
    mlt.setFieldNames(new String[]{"text"});

    // perform MLT query
    String likeText = "";
    for (String text : generateStrSeq(0, numDocs)) {
      likeText += text + " ";
    }
    BooleanQuery query = (BooleanQuery) mlt.like("text", new StringReader(likeText));

    // check best terms are topN of highest idf
    Collection<BooleanClause> clauses = query.clauses();
    assertEquals("Expected" + topN + "clauses only!", topN, clauses.size());

    Term[] expectedTerms = new Term[topN];
    int idx = 0;
    for (String text : generateStrSeq(numDocs - topN, topN)) {
      expectedTerms[idx++] = new Term("text", text);
    }
    for (BooleanClause clause : clauses) {
      Term term = ((TermQuery) clause.getQuery()).getTerm();
      assertTrue(Arrays.asList(expectedTerms).contains(term));
    }

    // clean up
    reader.close();
    dir.close();
    analyzer.close();
  }

  private String[] generateStrSeq(int from, int size) {
    String[] generatedStrings = new String[size];
    for (int i = 0; i < generatedStrings.length; i++) {
      generatedStrings[i] = String.valueOf(from + i);
    }
    return generatedStrings;
  }

  // TODO: add tests for the MoreLikeThisQuery
}
