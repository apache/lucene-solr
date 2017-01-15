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
package org.apache.lucene.expressions;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortRescorer;

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

  /** Uses the provided {@link Expression} to assign second
   *  pass scores. */
  public ExpressionRescorer(Expression expression, Bindings bindings) {
    super(new Sort(expression.getSortField(bindings, true)));
    this.expression = expression;
    this.bindings = bindings;
  }

  private static DoubleValues scores(int doc, float score) {
    return new DoubleValues() {
      @Override
      public double doubleValue() throws IOException {
        return score;
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        assert doc == target;
        return true;
      }
    };
  }

  @Override
  public Explanation explain(IndexSearcher searcher, Explanation firstPassExplanation, int docID) throws IOException {
    Explanation superExpl = super.explain(searcher, firstPassExplanation, docID);

    List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
    int subReader = ReaderUtil.subIndex(docID, leaves);
    LeafReaderContext readerContext = leaves.get(subReader);
    int docIDInSegment = docID - readerContext.docBase;

    DoubleValues scores = scores(docIDInSegment, firstPassExplanation.getValue());

    List<Explanation> subs = new ArrayList<>(Arrays.asList(superExpl.getDetails()));
    for(String variable : expression.variables) {
      DoubleValues dv = bindings.getDoubleValuesSource(variable).getValues(readerContext, scores);
      if (dv.advanceExact(docIDInSegment))
        subs.add(Explanation.match((float) dv.doubleValue(), "variable \"" + variable + "\""));
    }

    return Explanation.match(superExpl.getValue(), superExpl.getDescription(), subs);
  }
}
