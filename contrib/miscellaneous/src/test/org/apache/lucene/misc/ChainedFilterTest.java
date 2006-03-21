package org.apache.lucene.misc;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.Calendar;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;
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
  private QueryFilter bobFilter;
  private QueryFilter sueFilter;

  public void setUp() throws Exception {
    directory = new RAMDirectory();
    IndexWriter writer =
       new IndexWriter(directory, new WhitespaceAnalyzer(), true);

    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(1041397200000L); // 2003 January 01

    for (int i = 0; i < MAX; i++) {
      Document doc = new Document();
      doc.add(new Field("key", "" + (i + 1), Field.Store.YES, Field.Index.UN_TOKENIZED));
      doc.add(new Field("owner", (i < MAX / 2) ? "bob" : "sue", Field.Store.YES, Field.Index.UN_TOKENIZED));
      doc.add(new Field("date", cal.getTime().toString(), Field.Store.YES, Field.Index.UN_TOKENIZED));
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

    bobFilter = new QueryFilter(
        new TermQuery(new Term("owner", "bob")));
    sueFilter = new QueryFilter(
        new TermQuery(new Term("owner", "sue")));
  }

  public void testSingleFilter() throws Exception {
    ChainedFilter chain = new ChainedFilter(
        new Filter[] {dateFilter});

    Hits hits = searcher.search(query, chain);
    assertEquals(MAX, hits.length());

    chain = new ChainedFilter(new Filter[] {bobFilter});
    hits = searcher.search(query, chain);
    assertEquals(MAX / 2, hits.length());
  }

  public void testOR() throws Exception {
    ChainedFilter chain = new ChainedFilter(
      new Filter[] {sueFilter, bobFilter});

    Hits hits = searcher.search(query, chain);
    assertEquals("OR matches all", MAX, hits.length());
  }

  public void testAND() throws Exception {
    ChainedFilter chain = new ChainedFilter(
      new Filter[] {dateFilter, bobFilter}, ChainedFilter.AND);

    Hits hits = searcher.search(query, chain);
    assertEquals("AND matches just bob", MAX / 2, hits.length());
    assertEquals("bob", hits.doc(0).get("owner"));
  }

  public void testXOR() throws Exception {
    ChainedFilter chain = new ChainedFilter(
      new Filter[]{dateFilter, bobFilter}, ChainedFilter.XOR);

    Hits hits = searcher.search(query, chain);
    assertEquals("XOR matches sue", MAX / 2, hits.length());
    assertEquals("sue", hits.doc(0).get("owner"));
  }

  public void testANDNOT() throws Exception {
    ChainedFilter chain = new ChainedFilter(
      new Filter[]{dateFilter, sueFilter},
        new int[] {ChainedFilter.AND, ChainedFilter.ANDNOT});

    Hits hits = searcher.search(query, chain);
    assertEquals("ANDNOT matches just bob",
        MAX / 2, hits.length());
    assertEquals("bob", hits.doc(0).get("owner"));
  }

  private Date parseDate(String s) throws ParseException {
    return new SimpleDateFormat("yyyy MMM dd").parse(s);
  }

}
