package org.apache.lucene.expressions;

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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortRescorer;
import org.apache.lucene.search.Weight;

/**
 * A {@link Rescorer} that uses an expression to re-score
 * first pass hits.  Functionally this is the same as {@link
 * SortRescorer} (if you build the {@link Sort} using {@link
 * Expression#getSortField}), except for the explain method
 * which gives more detail by showing the value of each
 * variable.
 * 
 * @lucene.experimental
 */

class ExpressionRescorer extends SortRescorer {

  private final Expression expression;
  private final Bindings bindings;

  /** Uses the provided {@link ValueSource} to assign second
   *  pass scores. */
  public ExpressionRescorer(Expression expression, Bindings bindings) {
    super(new Sort(expression.getSortField(bindings, true)));
    this.expression = expression;
    this.bindings = bindings;
  }

  private static class FakeScorer extends Scorer {
    float score;
    int doc = -1;
    int freq = 1;

    public FakeScorer() {
      super(null);
    }
    
    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException("FakeScorer doesn't support advance(int)");
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int freq() {
      return freq;
    }

    @Override
    public int nextDoc() {
      throw new UnsupportedOperationException("FakeScorer doesn't support nextDoc()");
    }
    
    @Override
    public float score() {
      return score;
    }

    @Override
    public long cost() {
      return 1;
    }

    @Override
    public Weight getWeight() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<ChildScorer> getChildren() {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public Explanation explain(IndexSearcher searcher, Explanation firstPassExplanation, int docID) throws IOException {
    Explanation result = super.explain(searcher, firstPassExplanation, docID);

    List<AtomicReaderContext> leaves = searcher.getIndexReader().leaves();
    int subReader = ReaderUtil.subIndex(docID, leaves);
    AtomicReaderContext readerContext = leaves.get(subReader);
    int docIDInSegment = docID - readerContext.docBase;
    Map<String,Object> context = new HashMap<>();

    FakeScorer fakeScorer = new FakeScorer();
    fakeScorer.score = firstPassExplanation.getValue();
    fakeScorer.doc = docIDInSegment;

    context.put("scorer", fakeScorer);

    for(String variable : expression.variables) {
      result.addDetail(new Explanation((float) bindings.getValueSource(variable).getValues(context, readerContext).doubleVal(docIDInSegment),
                                       "variable \"" + variable + "\""));
    }

    return result;
  }
}
