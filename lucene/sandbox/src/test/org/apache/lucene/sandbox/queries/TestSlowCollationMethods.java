package org.apache.lucene.sandbox.queries;

import java.io.IOException;
import java.text.Collator;
import java.util.Locale;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

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

/**
 * Tests SlowCollatedStringComparator, SlowCollatedTermRangeQuery, and SlowCollatedTermRangeFilter
 */
public class TestSlowCollationMethods extends LuceneTestCase {
  private static Collator collator;
  private static IndexSearcher searcher;
  private static IndexReader reader;
  private static Directory dir;
  private static int numDocs;
  private static String splitDoc;

  @BeforeClass
  public static void beforeClass() throws Exception {
    final Locale locale = LuceneTestCase.randomLocale(random());
    collator = Collator.getInstance(locale);
    collator.setStrength(Collator.IDENTICAL);
    collator.setDecomposition(Collator.NO_DECOMPOSITION);

    numDocs = 1000 * RANDOM_MULTIPLIER;
    dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      String value = _TestUtil.randomUnicodeString(random());
      Field field = newField("field", value, StringField.TYPE_STORED);
      doc.add(field);
      iw.addDocument(doc);
    }
    splitDoc = _TestUtil.randomUnicodeString(random());
    reader = iw.getReader();
    iw.close();

    searcher = newSearcher(reader);
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    dir.close();
    collator = null;
    searcher = null;
    reader = null;
    dir = null;
  }
  
  private void doCheckSorting(TopDocs docs) throws Exception {
    String prev = "";
    for (ScoreDoc doc : docs.scoreDocs) {
      String value = reader.document(doc.doc).get("field");
      assertTrue(collator.compare(value, prev) >= 0);
      prev = value;
    }
  }
  
  public void testSort() throws Exception {
    SortField sf = new SortField("field", new FieldComparatorSource() {
      @Override
      public FieldComparator<String> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
        return new SlowCollatedStringComparator(numHits, fieldname, collator);
      }
    });
    final Sort sort = new Sort(sf);
    
    final TopDocs docs1 = searcher.search(TermRangeQuery.newStringRange("field", null, splitDoc, true, true), null, numDocs/(1+random().nextInt(4)), sort);
    doCheckSorting(docs1);
    
    final TopDocs docs2 = searcher.search(TermRangeQuery.newStringRange("field", splitDoc, null, true, true), null, numDocs/(1+random().nextInt(4)), sort);
    doCheckSorting(docs2);
    
    final TopDocs docs = TopDocs.merge(sort, numDocs/(1+random().nextInt(4)), new TopDocs[]{docs1, docs2});
    doCheckSorting(docs);
  }
  
  private void doTestRanges(String startPoint, String endPoint, Query query) throws Exception {
    // positive test
    TopDocs docs = searcher.search(query, numDocs);
    for (ScoreDoc doc : docs.scoreDocs) {
      String value = reader.document(doc.doc).get("field");
      assertTrue(collator.compare(value, startPoint) >= 0);
      assertTrue(collator.compare(value, endPoint) <= 0);
    }
    
    // negative test
    BooleanQuery bq = new BooleanQuery();
    bq.add(new MatchAllDocsQuery(), Occur.SHOULD);
    bq.add(query, Occur.MUST_NOT);
    docs = searcher.search(bq, numDocs);
    for (ScoreDoc doc : docs.scoreDocs) {
      String value = reader.document(doc.doc).get("field");
      assertTrue(collator.compare(value, startPoint) < 0 || collator.compare(value, endPoint) > 0);
    }
  }
  
  public void testRangeQuery() throws Exception {
    int numQueries = 50*RANDOM_MULTIPLIER;
    for (int i = 0; i < numQueries; i++) {
      String startPoint = _TestUtil.randomUnicodeString(random());
      String endPoint = _TestUtil.randomUnicodeString(random());
      Query query = new SlowCollatedTermRangeQuery("field", startPoint, endPoint, true, true, collator);
      doTestRanges(startPoint, endPoint, query);
    }
  }
  
  public void testRangeFilter() throws Exception {
    int numQueries = 50*RANDOM_MULTIPLIER;
    for (int i = 0; i < numQueries; i++) {
      String startPoint = _TestUtil.randomUnicodeString(random());
      String endPoint = _TestUtil.randomUnicodeString(random());
      Query query = new ConstantScoreQuery(new SlowCollatedTermRangeFilter("field", startPoint, endPoint, true, true, collator));
      doTestRanges(startPoint, endPoint, query);
    }
  }
}
