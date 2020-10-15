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

package org.apache.lucene.queries.function;

import java.io.IOException;
import java.util.function.DoublePredicate;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.apache.lucene.queries.function.FunctionMatchQuery.DEFAULT_MATCH_COST;

public class TestFunctionMatchQuery extends FunctionTestSetup {

  static IndexReader reader;
  static IndexSearcher searcher;
  private static final DoubleValuesSource in = DoubleValuesSource.fromFloatField(FLOAT_FIELD);

  @BeforeClass
  public static void beforeClass() throws Exception {
    createIndex(true);
    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
  }

  public void testRangeMatching() throws IOException {
    FunctionMatchQuery fmq = new FunctionMatchQuery(in, d -> d >= 2 && d < 4);
    TopDocs docs = searcher.search(fmq, 10);

    assertEquals(2, docs.totalHits.value);
    assertEquals(9, docs.scoreDocs[0].doc);
    assertEquals(13, docs.scoreDocs[1].doc);

    QueryUtils.check(random(), fmq, searcher, rarely());

  }

  public void testTwoPhaseIteratorMatchCost() throws IOException {
    DoublePredicate predicate = d -> true;

    // should use default match cost
    FunctionMatchQuery fmq = new FunctionMatchQuery(in, predicate);
    assertEquals(DEFAULT_MATCH_COST, getMatchCost(fmq), 0.1);

    // should use client defined match cost
    fmq = new FunctionMatchQuery(in, predicate, 200);
    assertEquals(200, getMatchCost(fmq), 0.1);
  }

  private static float getMatchCost(FunctionMatchQuery fmq) throws IOException {
    LeafReaderContext ctx = reader.leaves().get(0);
    return fmq.createWeight(searcher, ScoreMode.TOP_DOCS, 1)
      .scorer(ctx)
      .twoPhaseIterator()
      .matchCost();
  }
}
