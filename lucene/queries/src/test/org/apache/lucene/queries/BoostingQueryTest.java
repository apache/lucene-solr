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
package org.apache.lucene.queries;

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

public class BoostingQueryTest extends LuceneTestCase {
  // TODO: this suite desperately needs more tests!

  public void testBoostingQueryEquals() {
    TermQuery q1 = new TermQuery(new Term("subject:", "java"));
    TermQuery q2 = new TermQuery(new Term("subject:", "java"));
    assertEquals("Two TermQueries with same attributes should be equal", q1, q2);
    BoostingQuery bq1 = new BoostingQuery(q1, q2, 0.1f);
    QueryUtils.check(bq1);
    BoostingQuery bq2 = new BoostingQuery(q1, q2, 0.1f);
    assertEquals("BoostingQuery with same attributes is not equal", bq1, bq2);
  }

  public void testRewrite() throws IOException {
    IndexReader reader = new MultiReader();
    BoostingQuery q = new BoostingQuery(new BooleanQuery.Builder().build(), new MatchAllDocsQuery(), 3);
    Query rewritten = new IndexSearcher(reader).rewrite(q);
    Query expectedRewritten = new BoostingQuery(new MatchNoDocsQuery(), new MatchAllDocsQuery(), 3);
    assertEquals(expectedRewritten, rewritten);
    assertSame(rewritten, rewritten.rewrite(reader));
  }

  public void testQueryScore() throws IOException {
    Directory dir = newDirectory();
    MockAnalyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, analyzer);
    String[] docs = new String[] {
        "foo bar",
        "foo",
        "foobar",
        "foo baz"
    };
    FieldType ft = new FieldType();
    ft.setStored(false);
    ft.setIndexOptions(IndexOptions.DOCS);
    ft.setOmitNorms(true);
    for (int i = 0; i < docs.length; i++) {
      Document doc = new Document();
      doc.add(newStringField("id", "" + i, Field.Store.YES));
      doc.add(new Field("field", docs[i], ft));
      w.addDocument(doc);
    }
    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);
    s.setSimilarity(new BooleanSimilarity());
    BoostingQuery query = new BoostingQuery(
        new TermQuery(new Term("field", "foo")),
        new TermQuery(new Term("field", "bar")),
        0.1f
    );
    TopDocs search = s.search(query, 10);
    assertEquals(3L, search.totalHits);
    assertEquals(3, search.scoreDocs.length);
    assertEquals(1.0f, search.scoreDocs[0].score, 0.0);
    assertEquals(1.0f, search.scoreDocs[1].score, 0.0);
    assertEquals(0.1f, search.scoreDocs[2].score, 0.0);
    QueryUtils.check(random(), query, s);
    IOUtils.close(r, w, dir, analyzer);
  }

}
