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
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.search.BooleanClause.Occur;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

public class TestConstantScoreScorer extends LuceneTestCase {
  private static final String FIELD = "f";
  private static final String[] VALUES = new String[]{"foo", "bar", "foo bar", "azerty"};

  public void testBasics() throws Exception {
    try (TestConstantScoreScorerIndex index = new TestConstantScoreScorerIndex()) {
      BooleanQuery query = new BooleanQuery.Builder()
          .add(new ConstantScoreQuery(new TermQuery(new Term(FIELD, "foo"))), Occur.MUST)
          .build();

      ConstantScoreScorer scorer = index.constantScoreScorer(query, 1f);
      assertThat(scorer.iterator().nextDoc(), equalTo(0));
      assertThat(scorer.score(), is(equalTo(1f)));
      assertThat(scorer.iterator().nextDoc(), equalTo(2));
      assertThat(scorer.score(), is(equalTo(1f)));
      assertThat(scorer.iterator().nextDoc(), equalTo(NO_MORE_DOCS));
    }
  }

  public void testWithMinCompetitiveScoreSet() throws Exception {
    try (TestConstantScoreScorerIndex index = new TestConstantScoreScorerIndex()) {
      BooleanQuery query = new BooleanQuery.Builder()
          .add(new ConstantScoreQuery(new TermQuery(new Term(FIELD, "foo"))), Occur.MUST)
          .build();

      ConstantScoreScorer scorer = index.constantScoreScorer(query, 1f);
      assertThat(scorer.iterator().nextDoc(), equalTo(0));
      assertThat(scorer.score(), is(equalTo(1f)));
      scorer.setMinCompetitiveScore(2f);
      assertThat(scorer.iterator().nextDoc(), equalTo(NO_MORE_DOCS));
    }
  }

  static class TestConstantScoreScorerIndex implements AutoCloseable {

    private final Directory directory;
    private final RandomIndexWriter writer;
    private final IndexReader reader;

    TestConstantScoreScorerIndex() throws IOException {
      directory = newDirectory();

      writer = new RandomIndexWriter(random(), directory,
          newIndexWriterConfig().setMergePolicy(newLogMergePolicy(random().nextBoolean())));

      for (String VALUE : VALUES) {
        Document doc = new Document();
        doc.add(newTextField(FIELD, VALUE, Field.Store.YES));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);

      reader = writer.getReader();
      writer.close();
    }

    ConstantScoreScorer constantScoreScorer(BooleanQuery query, float score) throws IOException {
      IndexSearcher searcher = newSearcher(reader);
      Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE, 1);
      LeafReaderContext context = searcher.getIndexReader().leaves().get(0);

      Scorer scorer = weight.scorer(context);

      return new ConstantScoreScorer(scorer.getWeight(), score, scorer.iterator());
    }

    @Override
    public void close() throws Exception {
      reader.close();
      directory.close();
    }
  }

}
