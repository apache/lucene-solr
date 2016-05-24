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
import java.util.Random;

import org.apache.lucene.index.IndexReader;

/** Assertion-enabled query. */
public final class AssertingQuery extends Query {

  private final Random random;
  private final Query in;

  /** Sole constructor. */
  public AssertingQuery(Random random, Query in) {
    this.random = random;
    this.in = in;
  }

  /** Wrap a query if necessary. */
  public static Query wrap(Random random, Query query) {
    return query instanceof AssertingQuery ? query : new AssertingQuery(random, query);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new AssertingWeight(new Random(random.nextLong()), in.createWeight(searcher, needsScores), needsScores);
  }

  @Override
  public String toString(String field) {
    return in.toString(field);
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           in.equals(((AssertingQuery) other).in);
  }

  @Override
  public int hashCode() {
    return -in.hashCode();
  }

  public Random getRandom() {
    return random;
  }

  public Query getIn() {
    return in;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    final Query rewritten = in.rewrite(reader);
    if (rewritten == in) {
      return super.rewrite(reader);
    } else {
      return wrap(new Random(random.nextLong()), rewritten);
    }
  }

}
