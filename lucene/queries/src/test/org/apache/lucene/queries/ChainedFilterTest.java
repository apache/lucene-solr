package org.apache.lucene.queries;

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

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeFilter;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class ChainedFilterTest extends LuceneTestCase {
  public static final int MAX = 500;

  private Directory directory;
  private IndexSearcher searcher;
  private IndexReader reader;
  private Query query;
  // private DateFilter dateFilter;   DateFilter was deprecated and removed
  private TermRangeFilter dateFilter;
  private QueryWrapperFilter bobFilter;
  private QueryWrapperFilter sueFilter;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    // we use the default Locale/TZ since LuceneTestCase randomizes it
    Calendar cal = new GregorianCalendar(TimeZone.getDefault(), Locale.getDefault());
    cal.clear();
    cal.setTimeInMillis(1041397200000L); // 2003 January 01

    for (int i = 0; i < MAX; i++) {
      Document doc = new Document();
      doc.add(newStringField("key", "" + (i + 1), Field.Store.YES));
      doc.add(newStringField("owner", (i < MAX / 2) ? "bob" : "sue", Field.Store.YES));
      doc.add(newStringField("date", cal.getTime().toString(), Field.Store.YES));
      writer.addDocument(doc);

      cal.add(Calendar.DATE, 1);
    }
    reader = writer.getReader();
    writer.close();

    searcher = newSearcher(reader);

    // query for everything to make life easier
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("owner", "bob")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("owner", "sue")), BooleanClause.Occur.SHOULD);
    query = bq;

    // date filter matches everything too
    //Date pastTheEnd = parseDate("2099 Jan 1");
    // dateFilter = DateFilter.Before("date", pastTheEnd);
    // just treat dates as strings and select the whole range for now...
    dateFilter = TermRangeFilter.newStringRange("date","","ZZZZ",true,true);

    bobFilter = new QueryWrapperFilter(
        new TermQuery(new Term("owner", "bob")));
    sueFilter = new QueryWrapperFilter(
        new TermQuery(new Term("owner", "sue")));
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }

  private ChainedFilter getChainedFilter(Filter[] chain, int[] logic) {
    if (logic == null) {
      return new ChainedFilter(chain);
    } else {
      return new ChainedFilter(chain, logic);
    }
  }

  private ChainedFilter getChainedFilter(Filter[] chain, int logic) {
    return new ChainedFilter(chain, logic);
  }

  
  public void testSingleFilter() throws Exception {
    ChainedFilter chain = getChainedFilter(new Filter[] {dateFilter}, null);

    int numHits = searcher.search(query, chain, 1000).totalHits;
    assertEquals(MAX, numHits);

    chain = new ChainedFilter(new Filter[] {bobFilter});
    numHits = searcher.search(query, chain, 1000).totalHits;
    assertEquals(MAX / 2, numHits);
    
    chain = getChainedFilter(new Filter[] {bobFilter}, new int[] {ChainedFilter.AND});
    TopDocs hits = searcher.search(query, chain, 1000);
    numHits = hits.totalHits;
    assertEquals(MAX / 2, numHits);
    assertEquals("bob", searcher.doc(hits.scoreDocs[0].doc).get("owner"));
    
    chain = getChainedFilter(new Filter[] {bobFilter}, new int[] {ChainedFilter.ANDNOT});
    hits = searcher.search(query, chain, 1000);
    numHits = hits.totalHits;
    assertEquals(MAX / 2, numHits);
    assertEquals("sue", searcher.doc(hits.scoreDocs[0].doc).get("owner"));
  }

  public void testOR() throws Exception {
    ChainedFilter chain = getChainedFilter(
      new Filter[] {sueFilter, bobFilter}, null);

    int numHits = searcher.search(query, chain, 1000).totalHits;
    assertEquals("OR matches all", MAX, numHits);
  }

  public void testAND() throws Exception {
    ChainedFilter chain = getChainedFilter(
      new Filter[] {dateFilter, bobFilter}, ChainedFilter.AND);

    TopDocs hits = searcher.search(query, chain, 1000);
    assertEquals("AND matches just bob", MAX / 2, hits.totalHits);
    assertEquals("bob", searcher.doc(hits.scoreDocs[0].doc).get("owner"));
  }

  public void testXOR() throws Exception {
    ChainedFilter chain = getChainedFilter(
      new Filter[]{dateFilter, bobFilter}, ChainedFilter.XOR);

    TopDocs hits = searcher.search(query, chain, 1000);
    assertEquals("XOR matches sue", MAX / 2, hits.totalHits);
    assertEquals("sue", searcher.doc(hits.scoreDocs[0].doc).get("owner"));
  }

  public void testANDNOT() throws Exception {
    ChainedFilter chain = getChainedFilter(
      new Filter[]{dateFilter, sueFilter},
        new int[] {ChainedFilter.AND, ChainedFilter.ANDNOT});

    TopDocs hits = searcher.search(query, chain, 1000);
    assertEquals("ANDNOT matches just bob",
        MAX / 2, hits.totalHits);
    assertEquals("bob", searcher.doc(hits.scoreDocs[0].doc).get("owner"));
    
    chain = getChainedFilter(
        new Filter[]{bobFilter, bobFilter},
          new int[] {ChainedFilter.ANDNOT, ChainedFilter.ANDNOT});

      hits = searcher.search(query, chain, 1000);
      assertEquals("ANDNOT bob ANDNOT bob matches all sues",
          MAX / 2, hits.totalHits);
      assertEquals("sue", searcher.doc(hits.scoreDocs[0].doc).get("owner"));
  }

  /*
  private Date parseDate(String s) throws ParseException {
    return new SimpleDateFormat("yyyy MMM dd", Locale.ROOT).parse(s);
  }
  */
  
  public void testWithCachingFilter() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    IndexReader reader = writer.getReader();
    writer.close();
  
    IndexSearcher searcher = newSearcher(reader);
  
    Query query = new TermQuery(new Term("none", "none"));
  
    QueryWrapperFilter queryFilter = new QueryWrapperFilter(query);
    CachingWrapperFilter cachingFilter = new CachingWrapperFilter(queryFilter);
  
    searcher.search(query, cachingFilter, 1);
  
    CachingWrapperFilter cachingFilter2 = new CachingWrapperFilter(queryFilter);
    Filter[] chain = new Filter[2];
    chain[0] = cachingFilter;
    chain[1] = cachingFilter2;
    ChainedFilter cf = new ChainedFilter(chain);
  
    // throws java.lang.ClassCastException: org.apache.lucene.util.OpenBitSet cannot be cast to java.util.BitSet
    searcher.search(new MatchAllDocsQuery(), cf, 1);
    reader.close();
    dir.close();
  }

}
