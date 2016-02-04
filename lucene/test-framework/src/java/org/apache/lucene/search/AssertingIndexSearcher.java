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
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;

/**
 * Helper class that adds some extra checks to ensure correct
 * usage of {@code IndexSearcher} and {@code Weight}.
 */
public class AssertingIndexSearcher extends IndexSearcher {
  final Random random;
  public  AssertingIndexSearcher(Random random, IndexReader r) {
    super(r);
    this.random = new Random(random.nextLong());
  }
  
  public  AssertingIndexSearcher(Random random, IndexReaderContext context) {
    super(context);
    this.random = new Random(random.nextLong());
  }
  
  public  AssertingIndexSearcher(Random random, IndexReader r, ExecutorService ex) {
    super(r, ex);
    this.random = new Random(random.nextLong());
  }
  
  public  AssertingIndexSearcher(Random random, IndexReaderContext context, ExecutorService ex) {
    super(context, ex);
    this.random = new Random(random.nextLong());
  }
  
  /** Ensures, that the returned {@code Weight} is not normalized again, which may produce wrong scores. */
  @Override
  public Weight createNormalizedWeight(Query query, boolean needsScores) throws IOException {
    final Weight w = super.createNormalizedWeight(query, needsScores);
    return new AssertingWeight(random, w, needsScores) {

      @Override
      public void normalize(float norm, float boost) {
        throw new IllegalStateException("Weight already normalized.");
      }

      @Override
      public float getValueForNormalization() {
        throw new IllegalStateException("Weight already normalized.");
      }

    };
  }

  @Override
  public Weight createWeight(Query query, boolean needsScores) throws IOException {
    // this adds assertions to the inner weights/scorers too
    return new AssertingWeight(random, super.createWeight(query, needsScores), needsScores);
  }

  @Override
  public Query rewrite(Query original) throws IOException {
    // TODO: use the more sophisticated QueryUtils.check sometimes!
    QueryUtils.check(original);
    Query rewritten = super.rewrite(original);
    QueryUtils.check(rewritten);
    return rewritten;
  }

  @Override
  protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
    assert weight instanceof AssertingWeight;
    super.search(leaves, weight, AssertingCollector.wrap(random, collector));
  }

  @Override
  public String toString() {
    return "AssertingIndexSearcher(" + super.toString() + ")";
  }

}
