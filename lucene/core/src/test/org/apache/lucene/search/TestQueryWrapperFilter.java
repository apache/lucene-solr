package org.apache.lucene.search;

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
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;

public class TestQueryWrapperFilter extends LuceneTestCase {

  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newField("field", "value", TextField.TYPE_UNSTORED));
    writer.addDocument(doc);
    IndexReader reader = writer.getReader();
    writer.close();

    TermQuery termQuery = new TermQuery(new Term("field", "value"));

    // should not throw exception with primitive query
    QueryWrapperFilter qwf = new QueryWrapperFilter(termQuery);

    IndexSearcher searcher = newSearcher(reader);
    TopDocs hits = searcher.search(new MatchAllDocsQuery(), qwf, 10);
    assertEquals(1, hits.totalHits);
    hits = searcher.search(new MatchAllDocsQuery(), new CachingWrapperFilter(qwf), 10);
    assertEquals(1, hits.totalHits);

    // should not throw exception with complex primitive query
    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(termQuery, Occur.MUST);
    booleanQuery.add(new TermQuery(new Term("field", "missing")),
        Occur.MUST_NOT);
    qwf = new QueryWrapperFilter(termQuery);

    hits = searcher.search(new MatchAllDocsQuery(), qwf, 10);
    assertEquals(1, hits.totalHits);
    hits = searcher.search(new MatchAllDocsQuery(), new CachingWrapperFilter(qwf), 10);
    assertEquals(1, hits.totalHits);

    // should not throw exception with non primitive Query (doesn't implement
    // Query#createWeight)
    qwf = new QueryWrapperFilter(new FuzzyQuery(new Term("field", "valu")));

    hits = searcher.search(new MatchAllDocsQuery(), qwf, 10);
    assertEquals(1, hits.totalHits);
    hits = searcher.search(new MatchAllDocsQuery(), new CachingWrapperFilter(qwf), 10);
    assertEquals(1, hits.totalHits);

    // test a query with no hits
    termQuery = new TermQuery(new Term("field", "not_exist"));
    qwf = new QueryWrapperFilter(termQuery);
    hits = searcher.search(new MatchAllDocsQuery(), qwf, 10);
    assertEquals(0, hits.totalHits);
    hits = searcher.search(new MatchAllDocsQuery(), new CachingWrapperFilter(qwf), 10);
    assertEquals(0, hits.totalHits);
    reader.close();
    dir.close();
  }

  public void testRandom() throws Exception {
    final Directory d = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), d);
    w.w.getConfig().setMaxBufferedDocs(17);
    final int numDocs = atLeast(100);
    final Set<String> aDocs = new HashSet<String>();
    for(int i=0;i<numDocs;i++) {
      final Document doc = new Document();
      final String v;
      if (random().nextInt(5) == 4) {
        v = "a";
        aDocs.add(""+i);
      } else {
        v = "b";
      }
      final Field f = newField("field", v, StringField.TYPE_UNSTORED);
      doc.add(f);
      doc.add(newField("id", ""+i, StringField.TYPE_STORED));
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
    final TopDocs hits = new IndexSearcher(r).search(new MatchAllDocsQuery(),
                                                     new QueryWrapperFilter(new TermQuery(new Term("field", "a"))),
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
      doc.add(newField("field", English.intToEnglish(i), StringField.TYPE_UNSTORED));
      writer.addDocument(doc);
    }
    
    IndexReader reader = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(reader);
    
    for (int i = 0; i < 1000; i++) {
      TermQuery termQuery = new TermQuery(new Term("field", English.intToEnglish(i)));
      QueryWrapperFilter qwf = new QueryWrapperFilter(termQuery);
      TopDocs td = searcher.search(new MatchAllDocsQuery(), qwf, 10);
      assertEquals(1, td.totalHits);
    }
    
    reader.close();
    dir.close();
  }
}
