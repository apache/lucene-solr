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
package org.apache.lucene.spatial.composite;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

/**
 * A Query that considers an "indexQuery" to have approximate results, and a follow-on
 * {@link ValueSource}/{@link FunctionValues#boolVal(int)} is called to verify each hit
 * from {@link TwoPhaseIterator#matches()}.
 *
 * @lucene.experimental
 */
public class CompositeVerifyQuery extends Query {
  final Query indexQuery;//approximation (matches more than needed)
  final ValueSource predicateValueSource;//we call boolVal(doc)

  public CompositeVerifyQuery(Query indexQuery, ValueSource predicateValueSource) {
    this.indexQuery = indexQuery;
    this.predicateValueSource = predicateValueSource;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    final Query rewritten = indexQuery.rewrite(reader);
    if (rewritten != indexQuery) {
      return new CompositeVerifyQuery(rewritten, predicateValueSource);
    }
    return super.rewrite(reader);
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }
  
  private boolean equalsTo(CompositeVerifyQuery other) {
    return indexQuery.equals(other.indexQuery) &&
           predicateValueSource.equals(other.predicateValueSource);
  }

  @Override
  public int hashCode() {
    int result = classHash();
    result = 31 * result + indexQuery.hashCode();
    result = 31 * result + predicateValueSource.hashCode();
    return result;
  }

  @Override
  public String toString(String field) {
    //TODO verify this looks good
    return getClass().getSimpleName() + "(" + indexQuery.toString(field) + ", " + predicateValueSource + ")";
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    final Weight indexQueryWeight = indexQuery.createWeight(searcher, false);//scores aren't unsupported
    final Map valueSourceContext = ValueSource.newContext(searcher);

    return new ConstantScoreWeight(this) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {

        final Scorer indexQueryScorer = indexQueryWeight.scorer(context);
        if (indexQueryScorer == null) {
          return null;
        }

        final FunctionValues predFuncValues = predicateValueSource.getValues(valueSourceContext, context);

        final TwoPhaseIterator twoPhaseIterator = new TwoPhaseIterator(indexQueryScorer.iterator()) {
          @Override
          public boolean matches() throws IOException {
            return predFuncValues.boolVal(indexQueryScorer.docID());
          }

          @Override
          public float matchCost() {
            return 100; // TODO: use cost of predFuncValues.boolVal()
          }
        };

        return new ConstantScoreScorer(this, score(), twoPhaseIterator);
      }
    };
  }
}
