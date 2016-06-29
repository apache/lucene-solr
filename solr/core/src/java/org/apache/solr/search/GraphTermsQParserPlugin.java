
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

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 *  The GraphTermsQuery builds a disjunction query from a list of terms. The terms are first filtered by the maxDocFreq parameter.
 *  This allows graph traversals to skip traversing high frequency nodes which is often desirable from a performance standpoint.
 *
 *   Syntax: {!graphTerms f=field maxDocFreq=10000}term1,term2,term3
 **/

public class GraphTermsQParserPlugin extends QParserPlugin {
  public static final String NAME = "graphTerms";

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() throws SyntaxError {
        String fname = localParams.get(QueryParsing.F);
        FieldType ft = req.getSchema().getFieldTypeNoEx(fname);
        int maxDocFreq = localParams.getInt("maxDocFreq", Integer.MAX_VALUE);
        String qstr = localParams.get(QueryParsing.V);//never null

        if (qstr.length() == 0) {
          return new MatchNoDocsQuery();
        }

        final String[] splitVals = qstr.split(",");

        Term[] terms = new Term[splitVals.length];
        BytesRefBuilder term = new BytesRefBuilder();
        for (int i = 0; i < splitVals.length; i++) {
          String stringVal = splitVals[i].trim();
          if (ft != null) {
            ft.readableToIndexed(stringVal, term);
          } else {
            term.copyChars(stringVal);
          }
          BytesRef ref = term.toBytesRef();
          terms[i] = new Term(fname, ref);
        }

        ArrayUtil.timSort(terms);
        return new ConstantScoreQuery(new GraphTermsQuery(fname, terms, maxDocFreq));
      }
    };
  }

  private class GraphTermsQuery extends Query implements ExtendedQuery {

    private Term[] queryTerms;
    private String field;
    private int maxDocFreq;
    private Object id;

    public GraphTermsQuery(String field, Term[] terms, int maxDocFreq) {
      this.maxDocFreq = maxDocFreq;
      this.field = field;
      this.queryTerms = terms;
      this.id = new Object();
    }

    //Just for cloning
    private GraphTermsQuery(String field, Term[] terms, int maxDocFreq, Object id) {
      this.field = field;
      this.queryTerms = terms;
      this.maxDocFreq = maxDocFreq;
      this.id = id;
    }

    public boolean getCache() {
      return false;
    }

    public boolean getCacheSep() {
      return false;
    }

    public void setCacheSep(boolean sep) {

    }

    public void setCache(boolean cache) {

    }

    public int getCost() {
      return 1; // Not a post filter. The GraphTermsQuery will typically be used as the main query.
    }

    public void setCost(int cost) {

    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      return this;
    }

    public int hashCode() {
      return 31 * classHash() + id.hashCode();
    }

    public boolean equals(Object other) {
      return sameClassAs(other) &&
             id == ((GraphTermsQuery) other).id;
    }

    public GraphTermsQuery clone() {
      GraphTermsQuery clone = new GraphTermsQuery(this.field,
                                                  this.queryTerms,
                                                  this.maxDocFreq,
                                                  this.id);
      return clone;
    }

    @Override
    public String toString(String defaultField) {
      StringBuilder builder = new StringBuilder();
      boolean first = true;
      for (Term term : this.queryTerms) {
        if (!first) {
          builder.append(',');
        }
        first = false;
        builder.append(term.toString());
      }

      return builder.toString();
    }

    private class WeightOrDocIdSet {
      final Weight weight;
      final DocIdSet set;

      WeightOrDocIdSet(DocIdSet bitset) {
        this.set = bitset;
        this.weight = null;
      }
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {

      List<TermContext> finalContexts = new ArrayList();
      List<Term> finalTerms = new ArrayList();
      List<LeafReaderContext> contexts = searcher.getTopReaderContext().leaves();
      TermContext[] termContexts = new TermContext[this.queryTerms.length];
      collectTermContext(searcher.getIndexReader(), contexts, termContexts, this.queryTerms);
      for(int i=0; i<termContexts.length; i++) {
        TermContext termContext = termContexts[i];
        if(termContext != null && termContext.docFreq() <= this.maxDocFreq) {
          finalContexts.add(termContext);
          finalTerms.add(queryTerms[i]);
        }
      }

      return new ConstantScoreWeight(this) {

        @Override
        public void extractTerms(Set<Term> terms) {
          // no-op
          // This query is for abuse cases when the number of terms is too high to
          // run efficiently as a BooleanQuery. So likewise we hide its terms in
          // order to protect highlighters
        }

        private WeightOrDocIdSet rewrite(LeafReaderContext context) throws IOException {
          final LeafReader reader = context.reader();
          final Fields fields = reader.fields();
          Terms terms = fields.terms(field);
          if(terms == null) {
            return new WeightOrDocIdSet(new BitDocIdSet(new FixedBitSet(reader.maxDoc()), 0));
          }
          TermsEnum  termsEnum = terms.iterator();
          PostingsEnum docs = null;
          DocIdSetBuilder builder = new DocIdSetBuilder(reader.maxDoc(), terms);
          for (int i=0; i<finalContexts.size(); i++) {
            TermContext termContext = finalContexts.get(i);
            TermState termState = termContext.get(context.ord);
            if(termState != null) {
              Term term = finalTerms.get(i);
              termsEnum.seekExact(term.bytes(), termContext.get(context.ord));
              docs = termsEnum.postings(docs, PostingsEnum.NONE);
              builder.add(docs);
            }
          }
          return new WeightOrDocIdSet(builder.build());
        }

        private Scorer scorer(DocIdSet set) throws IOException {
          if (set == null) {
            return null;
          }
          final DocIdSetIterator disi = set.iterator();
          if (disi == null) {
            return null;
          }
          return new ConstantScoreScorer(this, score(), disi);
        }

        @Override
        public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
          final WeightOrDocIdSet weightOrBitSet = rewrite(context);
          if (weightOrBitSet.weight != null) {
            return weightOrBitSet.weight.bulkScorer(context);
          } else {
            final Scorer scorer = scorer(weightOrBitSet.set);
            if (scorer == null) {
              return null;
            }
            return new DefaultBulkScorer(scorer);
          }
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          final WeightOrDocIdSet weightOrBitSet = rewrite(context);
          if (weightOrBitSet.weight != null) {
            return weightOrBitSet.weight.scorer(context);
          } else {
            return scorer(weightOrBitSet.set);
          }
        }
      };
    }

    private void collectTermContext(IndexReader reader,
                                    List<LeafReaderContext> leaves,
                                    TermContext[] contextArray,
                                    Term[] queryTerms) throws IOException {
      TermsEnum termsEnum = null;
      for (LeafReaderContext context : leaves) {

        Terms terms = context.reader().terms(this.field);
        if (terms == null) {
          // field does not exist
          continue;
        }

        termsEnum = terms.iterator();

        if (termsEnum == TermsEnum.EMPTY) continue;

        for (int i = 0; i < queryTerms.length; i++) {
          Term term = queryTerms[i];
          TermContext termContext = contextArray[i];

          if (termsEnum.seekExact(term.bytes())) {
            if (termContext == null) {
              contextArray[i] = new TermContext(reader.getContext(),
                  termsEnum.termState(), context.ord, termsEnum.docFreq(),
                  termsEnum.totalTermFreq());
            } else {
              termContext.register(termsEnum.termState(), context.ord,
                  termsEnum.docFreq(), termsEnum.totalTermFreq());
            }
          }
        }
      }
    }
  }
}
