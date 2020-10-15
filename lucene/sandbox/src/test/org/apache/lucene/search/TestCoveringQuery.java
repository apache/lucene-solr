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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestCoveringQuery extends LuceneTestCase {

  public void testEquals() {
    TermQuery tq1 = new TermQuery(new Term("foo", "bar"));
    TermQuery tq2 = new TermQuery(new Term("foo", "quux"));
    LongValuesSource vs = LongValuesSource.fromLongField("field");

    CoveringQuery q1 = new CoveringQuery(Arrays.asList(tq1, tq2), vs);
    CoveringQuery q2 = new CoveringQuery(Arrays.asList(tq1, tq2), vs);
    QueryUtils.checkEqual(q1, q2);

    // order does not matter
    CoveringQuery q3 = new CoveringQuery(Arrays.asList(tq2, tq1), vs);
    QueryUtils.checkEqual(q1, q3);

    // values source matters
    CoveringQuery q4 = new CoveringQuery(Arrays.asList(tq2, tq1), LongValuesSource.fromLongField("other_field"));
    QueryUtils.checkUnequal(q1, q4);

    // duplicates matter
    CoveringQuery q5 = new CoveringQuery(Arrays.asList(tq1, tq1, tq2), vs);
    CoveringQuery q6 = new CoveringQuery(Arrays.asList(tq1, tq2, tq2), vs);
    QueryUtils.checkUnequal(q5, q6);

    // query matters
    CoveringQuery q7 = new CoveringQuery(Arrays.asList(tq1), vs);
    CoveringQuery q8 = new CoveringQuery(Arrays.asList(tq2), vs);
    QueryUtils.checkUnequal(q7, q8);
  }

  public void testRewrite() throws IOException {
    PhraseQuery pq = new PhraseQuery("foo", "bar");
    TermQuery tq = new TermQuery(new Term("foo", "bar"));
    LongValuesSource vs = LongValuesSource.fromIntField("field");
    assertEquals(
        new CoveringQuery(Collections.singleton(tq), vs),
        new CoveringQuery(Collections.singleton(pq), vs).rewrite(new MultiReader()));
  }

  public void testToString() {
    TermQuery tq1 = new TermQuery(new Term("foo", "bar"));
    TermQuery tq2 = new TermQuery(new Term("foo", "quux"));
    LongValuesSource vs = LongValuesSource.fromIntField("field");
    CoveringQuery q = new CoveringQuery(Arrays.asList(tq1, tq2), vs);
    assertEquals("CoveringQuery(queries=[foo:bar, foo:quux], minimumNumberMatch=long(field))", q.toString());
    assertEquals("CoveringQuery(queries=[bar, quux], minimumNumberMatch=long(field))", q.toString("foo"));
  }

  public void testRandom() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    int numDocs = atLeast(50);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      if (random().nextBoolean()) {
        doc.add(new StringField("field", "A", Store.NO));
      }
      if (random().nextBoolean()) {
        doc.add(new StringField("field", "B", Store.NO));
      }
      if (random().nextDouble() > 0.9) {
        doc.add(new StringField("field", "C", Store.NO));
      }
      if (random().nextDouble() > 0.1) {
        doc.add(new StringField("field", "D", Store.NO));
      }
      doc.add(new NumericDocValuesField("min_match", random().nextInt(6)));
      w.addDocument(doc);
    }

    IndexReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);
    w.close();

    int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      List<Query> queries = new ArrayList<>();
      if (random().nextBoolean()) {
        queries.add(new TermQuery(new Term("field", "A")));
      }
      if (random().nextBoolean()) {
        queries.add(new TermQuery(new Term("field", "B")));
      }
      if (random().nextBoolean()) {
        queries.add(new TermQuery(new Term("field", "C")));
      }
      if (random().nextBoolean()) {
        queries.add(new TermQuery(new Term("field", "D")));
      }
      if (random().nextBoolean()) {
        queries.add(new TermQuery(new Term("field", "E")));
      }

      Query q = new CoveringQuery(queries, LongValuesSource.fromLongField("min_match"));
      QueryUtils.check(random(), q, searcher);

      for (int i = 1; i < 4; ++i) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder()
            .setMinimumNumberShouldMatch(i);
        for (Query query : queries) {
          builder.add(query, Occur.SHOULD);
        }
        Query q1 = builder.build();
        Query q2 = new CoveringQuery(queries, LongValuesSource.constant(i));
        assertEquals(
            searcher.count(q1),
            searcher.count(q2));
      }

      Query filtered = new BooleanQuery.Builder()
          .add(q, Occur.MUST)
          .add(new TermQuery(new Term("field", "A")), Occur.MUST)
          .build();
      QueryUtils.check(random(), filtered, searcher);
    }
    
    r.close();
    dir.close();
  }
}
