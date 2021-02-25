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
package org.apache.solr.search;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.ValueSourceScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.solr.search.function.ValueSourceRangeFilter;

// This class works as either a normal constant score query, or as a PostFilter using a collector
public class FunctionRangeQuery extends SolrConstantScoreQuery implements PostFilter {

  final ValueSourceRangeFilter rangeFilt;

  public FunctionRangeQuery(ValueSourceRangeFilter filter) {
    super(filter);
    this.rangeFilt = filter;
    this.cost = 100; // default behavior should be PostFiltering
  }

  @Override
  public DelegatingCollector getFilterCollector(IndexSearcher searcher) {
    @SuppressWarnings({"rawtypes"})
    Map fcontext = ValueSource.newContext(searcher);
    Weight weight = rangeFilt.createWeight(searcher, ScoreMode.COMPLETE, 1);
    return new FunctionRangeCollector(fcontext, weight);
  }

  class FunctionRangeCollector extends DelegatingCollector {
    @SuppressWarnings({"rawtypes"})
    final Map fcontext;
    final Weight weight;
    ValueSourceScorer scorer;
    int maxdoc;

    @SuppressWarnings({"rawtypes"})
    public FunctionRangeCollector(Map fcontext, Weight weight) {
      this.fcontext = fcontext;
      this.weight = weight;
    }

    @Override
    public void collect(int doc) throws IOException {
      assert doc < maxdoc;
      if (scorer.matches(doc)) {
        leafDelegate.collect(doc);
      }
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      maxdoc = context.reader().maxDoc();
      @SuppressWarnings({"unchecked"})
      FunctionValues dv = rangeFilt.getValueSource().getValues(fcontext, context);
      scorer = dv.getRangeScorer(weight, context, rangeFilt.getLowerVal(), rangeFilt.getUpperVal(), rangeFilt.isIncludeLower(), rangeFilt.isIncludeUpper());
    }
  }
}
