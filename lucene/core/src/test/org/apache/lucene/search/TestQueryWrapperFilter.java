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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;

public class TestQueryWrapperFilter extends LuceneTestCase {

  // a filter for which other queries don't have special rewrite rules
  private static class FilterWrapper extends Filter {

    private final Filter in;
    
    FilterWrapper(Filter in) {
      this.in = in;
    }
    
    @Override
    public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
      return in.getDocIdSet(context, acceptDocs);
    }

    @Override
    public String toString(String field) {
      return in.toString(field);
    }
    
    @Override
    public boolean equals(Object obj) {
      if (super.equals(obj) == false) {
        return false;
      }
      return in.equals(((FilterWrapper) obj).in);
    }

    @Override
    public int hashCode() {
      return 31 * super.hashCode() + in.hashCode();
    }
  }

  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "value", Field.Store.NO));
    writer.addDocument(doc);
    IndexReader reader = writer.getReader();
    writer.close();

    TermQuery termQuery = new TermQuery(new Term("field", "value"));

    // should not throw exception with primitive query
    QueryWrapperFilter qwf = new QueryWrapperFilter(termQuery);

    IndexSearcher searcher = newSearcher(reader);
    TopDocs hits = searcher.search(new FilteredQuery(new MatchAllDocsQuery(), qwf), 10);
    assertEquals(1, hits.totalHits);
    hits = searcher.search(new FilteredQuery(new MatchAllDocsQuery(), new FilterWrapper(qwf)), 10);
    assertEquals(1, hits.totalHits);

    // should not throw exception with complex primitive query
    BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
    booleanQuery.add(termQuery, Occur.MUST);
    booleanQuery.add(new TermQuery(new Term("field", "missing")),
        Occur.MUST_NOT);
    qwf = new QueryWrapperFilter(termQuery);

    hits = searcher.search(new FilteredQuery(new MatchAllDocsQuery(), qwf), 10);
    assertEquals(1, hits.totalHits);
    hits = searcher.search(new FilteredQuery(new MatchAllDocsQuery(), new FilterWrapper(qwf)), 10);
    assertEquals(1, hits.totalHits);

    // should not throw exception with non primitive Query (doesn't implement
    // Query#createWeight)
    qwf = new QueryWrapperFilter(new FuzzyQuery(new Term("field", "valu")));

    hits = searcher.search(new FilteredQuery(new MatchAllDocsQuery(), qwf), 10);
    assertEquals(1, hits.totalHits);
    hits = searcher.search(new FilteredQuery(new MatchAllDocsQuery(), new FilterWrapper(qwf)), 10);
    assertEquals(1, hits.totalHits);

    // test a query with no hits
    termQuery = new TermQuery(new Term("field", "not_exist"));
    qwf = new QueryWrapperFilter(termQuery);
    hits = searcher.search(new FilteredQuery(new MatchAllDocsQuery(), qwf), 10);
    assertEquals(0, hits.totalHits);
    hits = searcher.search(new FilteredQuery(new MatchAllDocsQuery(), new FilterWrapper(qwf)), 10);
    assertEquals(0, hits.totalHits);
    reader.close();
    dir.close();
  }

  public void testRandom() throws Exception {
    final Directory d = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), d);
    w.w.getConfig().setMaxBufferedDocs(17);
    final int numDocs = atLeast(100);
    final Set<String> aDocs = new HashSet<>();
    for(int i=0;i<numDocs;i++) {
      final Document doc = new Document();
      final String v;
      if (random().nextInt(5) == 4) {
        v = "a";
        aDocs.add(""+i);
      } else {
        v = "b";
      }
      final Field f = newStringField("field", v, Field.Store.NO);
      doc.add(f);
      doc.add(newStringField("id", ""+i, Field.Store.YES));
      w.addDocument(doc);
    }

    final int numDelDocs = atLeast(10);
    for(int i=0;i<numDelDocs;i++) {
      final String delID = ""+random().nextInt(numDocs);
      w.deleteDocuments(new Term("id", delID));
      aDocs.remove(delID);
    }

    final IndexReader r = w.getReader();
    w.close();
    final TopDocs hits = newSearcher(r).search(new FilteredQuery(new MatchAllDocsQuery(),
                                                     new QueryWrapperFilter(new TermQuery(new Term("field", "a")))),
                                                     numDocs);
    assertEquals(aDocs.size(), hits.totalHits);
    for(ScoreDoc sd: hits.scoreDocs) {
      assertTrue(aDocs.contains(r.document(sd.doc).get("id")));
    }
    r.close();
    d.close();
  }
  
  public void testThousandDocuments() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < 1000; i++) {
      Document doc = new Document();
      doc.add(newStringField("field", English.intToEnglish(i), Field.Store.NO));
      writer.addDocument(doc);
    }
    
    IndexReader reader = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(reader);
    
    for (int i = 0; i < 1000; i++) {
      TermQuery termQuery = new TermQuery(new Term("field", English.intToEnglish(i)));
      QueryWrapperFilter qwf = new QueryWrapperFilter(termQuery);
      TopDocs td = searcher.search(new FilteredQuery(new MatchAllDocsQuery(), qwf), 10);
      assertEquals(1, td.totalHits);
    }
    
    reader.close();
    dir.close();
  }

  public void testScore() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new StringField("foo", "bar", Store.NO));
    writer.addDocument(doc);
    writer.commit();
    final IndexReader reader = writer.getReader();
    writer.close();
    final IndexSearcher searcher = new IndexSearcher(reader);
    final Query query = new QueryWrapperFilter(new TermQuery(new Term("foo", "bar")));
    final TopDocs topDocs = searcher.search(query, 1);
    assertEquals(1, topDocs.totalHits);
    assertEquals(0f, topDocs.scoreDocs[0].score, 0f);
    reader.close();
    dir.close();
  }

  public void testQueryWrapperFilterPropagatesApproximations() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new StringField("foo", "bar", Store.NO));
    writer.addDocument(doc);
    writer.commit();
    final IndexReader reader = writer.getReader();
    writer.close();
    final IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null); // to still have approximations
    final Query query = new QueryWrapperFilter(new RandomApproximationQuery(new TermQuery(new Term("foo", "bar")), random()));
    final Weight weight = searcher.createNormalizedWeight(query, random().nextBoolean());
    final Scorer scorer = weight.scorer(reader.leaves().get(0));
    assertNotNull(scorer.twoPhaseIterator());
    reader.close();
    dir.close();
  }

  public void testBasics() {
    QueryUtils.check(new QueryWrapperFilter(new TermQuery(new Term("foo", "bar"))));
  }
}
