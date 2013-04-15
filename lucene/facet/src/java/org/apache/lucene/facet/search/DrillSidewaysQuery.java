package org.apache.lucene.facet.search;

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
import java.util.Arrays;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;

class DrillSidewaysQuery extends Query {
  final Query baseQuery;
  final Collector drillDownCollector;
  final Collector[] drillSidewaysCollectors;
  final Term[][] drillDownTerms;

  DrillSidewaysQuery(Query baseQuery, Collector drillDownCollector, Collector[] drillSidewaysCollectors, Term[][] drillDownTerms) {
    this.baseQuery = baseQuery;
    this.drillDownCollector = drillDownCollector;
    this.drillSidewaysCollectors = drillSidewaysCollectors;
    this.drillDownTerms = drillDownTerms;
  }

  @Override
  public String toString(String field) {
    return "DrillSidewaysQuery";
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query newQuery = baseQuery;
    while(true) {
      Query rewrittenQuery = newQuery.rewrite(reader);
      if (rewrittenQuery == newQuery) {
        break;
      }
      newQuery = rewrittenQuery;
    }
    if (newQuery == baseQuery) {
      return this;
    } else {
      return new DrillSidewaysQuery(newQuery, drillDownCollector, drillSidewaysCollectors, drillDownTerms);
    }
  }
  
  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    final Weight baseWeight = baseQuery.createWeight(searcher);

    return new Weight() {
      @Override
      public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
        return baseWeight.explain(context, doc);
      }

      @Override
      public Query getQuery() {
        return baseQuery;
      }

      @Override
      public float getValueForNormalization() throws IOException {
        return baseWeight.getValueForNormalization();
      }

      @Override
      public void normalize(float norm, float topLevelBoost) {
        baseWeight.normalize(norm, topLevelBoost);
      }

      @Override
      public boolean scoresDocsOutOfOrder() {
        // TODO: would be nice if AssertingIndexSearcher
        // confirmed this for us
        return false;
      }

      @Override
      public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder,
                           boolean topScorer, Bits acceptDocs) throws IOException {

        DrillSidewaysScorer.DocsEnumsAndFreq[] dims = new DrillSidewaysScorer.DocsEnumsAndFreq[drillDownTerms.length];
        TermsEnum termsEnum = null;
        String lastField = null;
        int nullCount = 0;
        for(int dim=0;dim<dims.length;dim++) {
          dims[dim] = new DrillSidewaysScorer.DocsEnumsAndFreq();
          dims[dim].sidewaysCollector = drillSidewaysCollectors[dim];
          String field = drillDownTerms[dim][0].field();
          dims[dim].dim = drillDownTerms[dim][0].text();
          if (lastField == null || !lastField.equals(field)) {
            AtomicReader reader = context.reader();
            Terms terms = reader.terms(field);
            if (terms != null) {
              termsEnum = terms.iterator(null);
            }
            lastField = field;
          }
          if (termsEnum == null) {
            nullCount++;
            continue;
          }
          dims[dim].docsEnums = new DocsEnum[drillDownTerms[dim].length];
          for(int i=0;i<drillDownTerms[dim].length;i++) {
            if (termsEnum.seekExact(drillDownTerms[dim][i].bytes(), false)) {
              dims[dim].freq = Math.max(dims[dim].freq, termsEnum.docFreq());
              dims[dim].docsEnums[i] = termsEnum.docs(null, null);
            }
          }
        }

        if (nullCount > 1) {
          return null;
        }

        // Sort drill-downs by most restrictive first:
        Arrays.sort(dims);

        // TODO: it could be better if we take acceptDocs
        // into account instead of baseScorer?
        Scorer baseScorer = baseWeight.scorer(context, scoreDocsInOrder, false, acceptDocs);

        if (baseScorer == null) {
          return null;
        }

        return new DrillSidewaysScorer(this, context,
                                       baseScorer,
                                       drillDownCollector, dims);
      }
    };
  }

  // TODO: these should do "deeper" equals/hash on the 2-D drillDownTerms array

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((baseQuery == null) ? 0 : baseQuery.hashCode());
    result = prime * result
        + ((drillDownCollector == null) ? 0 : drillDownCollector.hashCode());
    result = prime * result + Arrays.hashCode(drillDownTerms);
    result = prime * result + Arrays.hashCode(drillSidewaysCollectors);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    DrillSidewaysQuery other = (DrillSidewaysQuery) obj;
    if (baseQuery == null) {
      if (other.baseQuery != null) return false;
    } else if (!baseQuery.equals(other.baseQuery)) return false;
    if (drillDownCollector == null) {
      if (other.drillDownCollector != null) return false;
    } else if (!drillDownCollector.equals(other.drillDownCollector)) return false;
    if (!Arrays.equals(drillDownTerms, other.drillDownTerms)) return false;
    if (!Arrays.equals(drillSidewaysCollectors, other.drillSidewaysCollectors)) return false;
    return true;
  }
}
