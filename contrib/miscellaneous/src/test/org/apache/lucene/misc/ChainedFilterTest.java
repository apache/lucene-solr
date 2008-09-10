package org.apache.lucene.misc;

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
import java.util.*;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.*;

public class ChainedFilterTest extends TestCase {
  public static final int MAX = 500;

  private RAMDirectory directory;
  private IndexSearcher searcher;
  private Query query;
  // private DateFilter dateFilter;   DateFilter was deprecated and removed
  private RangeFilter dateFilter;
  private QueryWrapperFilter bobFilter;
  private QueryWrapperFilter sueFilter;

  public void setUp() throws Exception {
    directory = new RAMDirectory();
    IndexWriter writer =
       new IndexWriter(directory, new WhitespaceAnalyzer(), true);

    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(1041397200000L); // 2003 January 01

    for (int i = 0; i < MAX; i++) {
      Document doc = new Document();
      doc.add(new Field("key", "" + (i + 1), Field.Store.YES, Field.Index.NOT_ANALYZED));
      doc.add(new Field("owner", (i < MAX / 2) ? "bob" : "sue", Field.Store.YES, Field.Index.NOT_ANALYZED));
      doc.add(new Field("date", cal.getTime().toString(), Field.Store.YES, Field.Index.NOT_ANALYZED));
      writer.addDocument(doc);

      cal.add(Calendar.DATE, 1);
    }

    writer.close();

    searcher = new IndexSearcher(directory);

    // query for everything to make life easier
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("owner", "bob")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("owner", "sue")), BooleanClause.Occur.SHOULD);
    query = bq;

    // date filter matches everything too
    Date pastTheEnd = parseDate("2099 Jan 1");
    // dateFilter = DateFilter.Before("date", pastTheEnd);
    // just treat dates as strings and select the whole range for now...
    dateFilter = new RangeFilter("date","","ZZZZ",true,true);

    bobFilter = new QueryWrapperFilter(
        new TermQuery(new Term("owner", "bob")));
    sueFilter = new QueryWrapperFilter(
        new TermQuery(new Term("owner", "sue")));
  }

  private Filter[] getChainWithOldFilters(Filter[] chain) {
    Filter[] oldFilters = new Filter[chain.length];
    for (int i = 0; i < chain.length; i++) {
      final Filter f = chain[i];
    // create old BitSet-based Filter as wrapper
      oldFilters[i] = new Filter() {
        public BitSet bits(IndexReader reader) throws IOException {
          BitSet bits = new BitSet(reader.maxDoc());
        DocIdSetIterator it = f.getDocIdSet(reader).iterator();          
          while(it.next()) {
            bits.set(it.doc());
          }
          return bits;
        }
    };
    }
    return oldFilters;
  }
  
  private ChainedFilter getChainedFilter(Filter[] chain, int[] logic, boolean old) {
    if (old) {
      chain = getChainWithOldFilters(chain);
    }
    
    if (logic == null) {
      return new ChainedFilter(chain);
    } else {
      return new ChainedFilter(chain, logic);
    }
  }

  private ChainedFilter getChainedFilter(Filter[] chain, int logic, boolean old) {
    if (old) {
      chain = getChainWithOldFilters(chain);
    }
    
    return new ChainedFilter(chain, logic);
  }

  
  public void testSingleFilter() throws Exception {
    for (int mode = 0; mode < 2; mode++) {
      boolean old = (mode==0);
      
      ChainedFilter chain = getChainedFilter(new Filter[] {dateFilter}, null, old);
  
      Hits hits = searcher.search(query, chain);
      assertEquals(MAX, hits.length());
  
      chain = new ChainedFilter(new Filter[] {bobFilter});
      hits = searcher.search(query, chain);
      assertEquals(MAX / 2, hits.length());
      
      chain = getChainedFilter(new Filter[] {bobFilter}, new int[] {ChainedFilter.AND}, old);
      hits = searcher.search(query, chain);
      assertEquals(MAX / 2, hits.length());
      assertEquals("bob", hits.doc(0).get("owner"));
      
      chain = getChainedFilter(new Filter[] {bobFilter}, new int[] {ChainedFilter.ANDNOT}, old);
      hits = searcher.search(query, chain);
      assertEquals(MAX / 2, hits.length());
      assertEquals("sue", hits.doc(0).get("owner"));
    }
  }

  public void testOR() throws Exception {
    for (int mode = 0; mode < 2; mode++) {
      boolean old = (mode==0);
      ChainedFilter chain = getChainedFilter(
        new Filter[] {sueFilter, bobFilter}, null, old);
  
      Hits hits = searcher.search(query, chain);
      assertEquals("OR matches all", MAX, hits.length());
    }
  }

  public void testAND() throws Exception {
    for (int mode = 0; mode < 2; mode++) {
      boolean old = (mode==0);
      ChainedFilter chain = getChainedFilter(
        new Filter[] {dateFilter, bobFilter}, ChainedFilter.AND, old);
  
      Hits hits = searcher.search(query, chain);
      assertEquals("AND matches just bob", MAX / 2, hits.length());
      assertEquals("bob", hits.doc(0).get("owner"));
    }
  }

  public void testXOR() throws Exception {
    for (int mode = 0; mode < 2; mode++) {
      boolean old = (mode==0);
      ChainedFilter chain = getChainedFilter(
        new Filter[]{dateFilter, bobFilter}, ChainedFilter.XOR, old);
  
      Hits hits = searcher.search(query, chain);
      assertEquals("XOR matches sue", MAX / 2, hits.length());
      assertEquals("sue", hits.doc(0).get("owner"));
    }
  }

  public void testANDNOT() throws Exception {
    for (int mode = 0; mode < 2; mode++) {
      boolean old = (mode==0);
      ChainedFilter chain = getChainedFilter(
        new Filter[]{dateFilter, sueFilter},
          new int[] {ChainedFilter.AND, ChainedFilter.ANDNOT}, old);
  
      Hits hits = searcher.search(query, chain);
      assertEquals("ANDNOT matches just bob",
          MAX / 2, hits.length());
      assertEquals("bob", hits.doc(0).get("owner"));
      
      chain = getChainedFilter(
          new Filter[]{bobFilter, bobFilter},
            new int[] {ChainedFilter.ANDNOT, ChainedFilter.ANDNOT}, old);
  
        hits = searcher.search(query, chain);
        assertEquals("ANDNOT bob ANDNOT bob matches all sues",
            MAX / 2, hits.length());
        assertEquals("sue", hits.doc(0).get("owner"));
    }
  }

  private Date parseDate(String s) throws ParseException {
    return new SimpleDateFormat("yyyy MMM dd", Locale.US).parse(s);
  }
  
  public void testWithCachingFilter() throws Exception {
    for (int mode = 0; mode < 2; mode++) {
      boolean old = (mode==0);
      Directory dir = new RAMDirectory();
      Analyzer analyzer = new WhitespaceAnalyzer();
  
      IndexWriter writer = new IndexWriter(dir, analyzer, true, MaxFieldLength.LIMITED);
      writer.close();
  
      Searcher searcher = new IndexSearcher(dir);
  
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
    }
  }

}
