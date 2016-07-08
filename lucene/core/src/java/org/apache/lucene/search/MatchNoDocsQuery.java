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
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;

/**
 * A query that matches no documents.
 */

public class MatchNoDocsQuery extends Query {

  private final String reason;

  /** Default constructor */
  public MatchNoDocsQuery() {
    this("");
  }

  /** Provides a reason explaining why this query was used */
  public MatchNoDocsQuery(String reason) {
    this.reason = reason;
  }
  
  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new Weight(this) {
      @Override
      public void extractTerms(Set<Term> terms) {
      }

      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return Explanation.noMatch(reason);
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        return null;
      }

      @Override
      public final float getValueForNormalization() throws IOException {
        return 0;
      }

      @Override
      public void normalize(float norm, float boost) {
      }

      /** Return the normalization factor for this weight. */
      protected final float queryNorm() {
        return 0;
      }

      /** Return the boost for this weight. */
      protected final float boost() {
        return 0;
      }

      /** Return the score produced by this {@link Weight}. */
      protected final float score() {
        return 0;
      }
    };
  }

  @Override
  public String toString(String field) {
    return "MatchNoDocsQuery(\"" + reason + "\")";
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o);
  }

  @Override
  public int hashCode() {
    return classHash();
  }
}
