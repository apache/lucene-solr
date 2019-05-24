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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestBlockMaxConjunction extends LuceneTestCase {

  private Query maybeWrap(Query query) {
    if (random().nextBoolean()) {
      query = new BlockScoreQueryWrapper(query, TestUtil.nextInt(random(), 2, 8));
      query = new AssertingQuery(random(), query);
    }
    return query;
  }

  private Query maybeWrapTwoPhase(Query query) {
    if (random().nextBoolean()) {
      query = new RandomApproximationQuery(query, random());
      query = new AssertingQuery(random(), query);
    }
    return query;
  }

  public void testRandom() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      int numValues = random().nextInt(1 << random().nextInt(5));
      int start = random().nextInt(10);
      for (int j = 0; j < numValues; ++j) {
        doc.add(new StringField("foo", Integer.toString(start + j), Store.NO));
      }
      w.addDocument(doc);
    }
    IndexReader reader = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = newSearcher(reader);

    for (int iter = 0; iter < 100; ++iter) {
      int start = random().nextInt(10);
      int numClauses = random().nextInt(1 << random().nextInt(5));
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      for (int i = 0; i < numClauses; ++i) {
        builder.add(maybeWrap(new TermQuery(new Term("foo", Integer.toString(start + i)))), Occur.MUST);
      }
      Query query = builder.build();

      CheckHits.checkTopScores(random(), query, searcher);

      int filterTerm = random().nextInt(30);
      Query filteredQuery = new BooleanQuery.Builder()
          .add(query, Occur.MUST)
          .add(new TermQuery(new Term("foo", Integer.toString(filterTerm))), Occur.FILTER)
          .build();

      CheckHits.checkTopScores(random(), filteredQuery, searcher);

      builder = new BooleanQuery.Builder();
      for (int i = 0; i < numClauses; ++i) {
        builder.add(maybeWrapTwoPhase(new TermQuery(new Term("foo", Integer.toString(start + i)))), Occur.MUST);
      }

      Query twoPhaseQuery = new BooleanQuery.Builder()
          .add(query, Occur.MUST)
          .add(new TermQuery(new Term("foo", Integer.toString(filterTerm))), Occur.FILTER)
          .build();

      CheckHits.checkTopScores(random(), twoPhaseQuery, searcher);
    }
    reader.close();
    dir.close();
  }

}
