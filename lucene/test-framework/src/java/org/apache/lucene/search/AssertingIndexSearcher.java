package org.apache.lucene.search;

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

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.util.TestUtil;

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
  public Weight createNormalizedWeight(Query query) throws IOException {
    final Weight w = super.createNormalizedWeight(query);
    return new AssertingWeight(random, w) {

      @Override
      public void normalize(float norm, float topLevelBoost) {
        throw new IllegalStateException("Weight already normalized.");
      }

      @Override
      public float getValueForNormalization() {
        throw new IllegalStateException("Weight already normalized.");
      }

    };
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
  protected Query wrapFilter(Query query, Filter filter) {
    if (random.nextBoolean())
      return super.wrapFilter(query, filter);
    return (filter == null) ? query : new FilteredQuery(query, filter, TestUtil.randomFilterStrategy(random));
  }

  @Override
  protected void search(List<AtomicReaderContext> leaves, Weight weight, Collector collector) throws IOException {
    // TODO: shouldn't we AssertingCollector.wrap(collector) here?
    super.search(leaves, AssertingWeight.wrap(random, weight), collector);
  }

  @Override
  public String toString() {
    return "AssertingIndexSearcher(" + super.toString() + ")";
  }

}
