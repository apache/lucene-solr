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
import java.util.Arrays;

import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.LuceneTestCase;

public class TestMaxClauseLimit extends LuceneTestCase {
  public void testFlattenInnerDisjunctionsWithMoreThan1024Terms() throws IOException {
    IndexSearcher searcher = newSearcher(new MultiReader());

    BooleanQuery.Builder builder1024 = new BooleanQuery.Builder();
    for(int i = 0; i < 1024; i++) {
      builder1024.add(new TermQuery(new Term("foo", "bar-" + i)), BooleanClause.Occur.SHOULD);
    }
    Query inner = builder1024.build();
    Query query = new BooleanQuery.Builder()
        .add(inner, BooleanClause.Occur.SHOULD)
        .add(new TermQuery(new Term("foo", "baz")), BooleanClause.Occur.SHOULD)
        .build();

    expectThrows(IndexSearcher.TooManyClauses.class, () -> {
      searcher.rewrite(query);
    });
  }

  public void testLargeTermsNestedFirst() throws IOException {
    IndexSearcher searcher = newSearcher(new MultiReader());
    BooleanQuery.Builder nestedBuilder = new BooleanQuery.Builder();

    nestedBuilder.setMinimumNumberShouldMatch(5);

    for(int i = 0; i < 600; i++) {
      nestedBuilder.add(new TermQuery(new Term("foo", "bar-" + i)), BooleanClause.Occur.SHOULD);
    }
    Query inner = nestedBuilder.build();
    BooleanQuery.Builder builderMixed = new BooleanQuery.Builder()
        .add(inner, BooleanClause.Occur.SHOULD);

    builderMixed.setMinimumNumberShouldMatch(5);

    for (int i = 0; i < 600; i++) {
      builderMixed.add(new TermQuery(new Term("foo", "bar")), BooleanClause.Occur.SHOULD);
    }

    Query query = builderMixed.build();

    expectThrows(IndexSearcher.TooManyClauses.class, () -> {
      searcher.rewrite(query);
    });
  }

  public void testLargeTermsNestedLast() throws IOException {
    IndexSearcher searcher = newSearcher(new MultiReader());
    BooleanQuery.Builder nestedBuilder = new BooleanQuery.Builder();

    nestedBuilder.setMinimumNumberShouldMatch(5);

    for(int i = 0; i < 600; i++) {
      nestedBuilder.add(new TermQuery(new Term("foo", "bar-" + i)), BooleanClause.Occur.SHOULD);
    }
    Query inner = nestedBuilder.build();
    BooleanQuery.Builder builderMixed = new BooleanQuery.Builder();

    builderMixed.setMinimumNumberShouldMatch(5);

    for (int i = 0; i < 600; i++) {
      builderMixed.add(new TermQuery(new Term("foo", "bar")), BooleanClause.Occur.SHOULD);
    }

    builderMixed.add(inner, BooleanClause.Occur.SHOULD);

    Query query = builderMixed.build();

    expectThrows(IndexSearcher.TooManyClauses.class, () -> {
      searcher.rewrite(query);
    });
  }

  public void testLargeDisjunctionMaxQuery() throws IOException {
    IndexSearcher searcher = newSearcher(new MultiReader());
    Query[] clausesQueryArray = new Query[1050];

    for(int i = 0; i < 1049; i++) {
      clausesQueryArray[i] = new TermQuery(new Term("field", "a"));
    }

    PhraseQuery pq = new PhraseQuery("field", new String[0]);

    clausesQueryArray[1049] = pq;

    DisjunctionMaxQuery dmq = new DisjunctionMaxQuery(
        Arrays.asList(clausesQueryArray),
        0.5f);

    expectThrows(IndexSearcher.TooManyClauses.class, () -> {
      searcher.rewrite(dmq);
    });
  }

  public void testMultiExactWithRepeats() throws IOException {
    IndexSearcher searcher = newSearcher(new MultiReader());
    MultiPhraseQuery.Builder qb = new MultiPhraseQuery.Builder();

    for (int i = 0;i < 1050; i++) {
      qb.add(new Term[]{new Term("foo", "bar-" + i), new Term("foo", "bar+" + i)}, 0);
    }

    expectThrows(IndexSearcher.TooManyClauses.class, () -> {
      searcher.rewrite(qb.build());
    });
  }
}
