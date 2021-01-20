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
import java.util.TreeSet;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;

public class IGainTermsQParserPlugin extends QParserPlugin {

  public static final String NAME = "igain";

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new IGainTermsQParser(qstr, localParams, params, req);
  }

  private static class IGainTermsQParser extends QParser {

    public IGainTermsQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      super(qstr, localParams, params, req);
    }

    @Override
    public Query parse() throws SyntaxError {

      String field = getParam("field");
      String outcome = getParam("outcome");
      int numTerms = Integer.parseInt(getParam("numTerms"));
      int positiveLabel = Integer.parseInt(getParam("positiveLabel"));

      return new IGainTermsQuery(field, outcome, positiveLabel, numTerms);
    }
  }

  private static class IGainTermsQuery extends AnalyticsQuery {

    private String field;
    private String outcome;
    private int numTerms;
    private int positiveLabel;

    public IGainTermsQuery(String field, String outcome, int positiveLabel, int numTerms) {
      this.field = field;
      this.outcome = outcome;
      this.numTerms = numTerms;
      this.positiveLabel = positiveLabel;
    }

    @Override
    public DelegatingCollector getAnalyticsCollector(ResponseBuilder rb, IndexSearcher searcher) {
      return new IGainTermsCollector(rb, searcher, field, outcome, positiveLabel, numTerms);
    }
  }

  private static class IGainTermsCollector extends DelegatingCollector {

    private String field;
    private String outcome;
    private IndexSearcher searcher;
    private ResponseBuilder rb;
    private int positiveLabel;
    private int numTerms;
    private int count;

    private NumericDocValues leafOutcomeValue;
    private SparseFixedBitSet positiveSet;
    private SparseFixedBitSet negativeSet;


    private int numPositiveDocs;


    public IGainTermsCollector(ResponseBuilder rb, IndexSearcher searcher, String field, String outcome, int positiveLabel, int numTerms) {
      this.rb = rb;
      this.searcher = searcher;
      this.field = field;
      this.outcome = outcome;
      this.positiveSet = new SparseFixedBitSet(searcher.getIndexReader().maxDoc());
      this.negativeSet = new SparseFixedBitSet(searcher.getIndexReader().maxDoc());

      this.numTerms = numTerms;
      this.positiveLabel = positiveLabel;
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      LeafReader reader = context.reader();
      leafOutcomeValue = reader.getNumericDocValues(outcome);
    }

    @Override
    public void collect(int doc) throws IOException {
      super.collect(doc);
      ++count;
      int value;
      if (leafOutcomeValue.advanceExact(doc)) {
        value = (int) leafOutcomeValue.longValue();
      } else {
        value = 0;
      }
      
      if (value == positiveLabel) {
        positiveSet.set(context.docBase + doc);
        numPositiveDocs++;
      } else {
        negativeSet.set(context.docBase + doc);
      }
    }

    @Override
    public void finish() throws IOException {
      NamedList<Double> analytics = new NamedList<Double>();
      @SuppressWarnings({"unchecked", "rawtypes"})
      NamedList<Integer> topFreq = new NamedList();

      @SuppressWarnings({"unchecked", "rawtypes"})
      NamedList<Integer> allFreq = new NamedList();

      rb.rsp.add("featuredTerms", analytics);
      rb.rsp.add("docFreq", topFreq);
      rb.rsp.add("numDocs", count);

      TreeSet<TermWithScore> topTerms = new TreeSet<>();

      double numDocs = count;
      double pc = numPositiveDocs / numDocs;
      double entropyC = binaryEntropy(pc);

      Terms terms = ((SolrIndexSearcher)searcher).getSlowAtomicReader().terms(field);
      TermsEnum termsEnum = terms == null ? TermsEnum.EMPTY : terms.iterator();
      BytesRef term;
      PostingsEnum postingsEnum = null;
      while ((term = termsEnum.next()) != null) {
        postingsEnum = termsEnum.postings(postingsEnum);
        int xc = 0;
        int nc = 0;
        while (postingsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          if (positiveSet.get(postingsEnum.docID())) {
            xc++;
          } else if (negativeSet.get(postingsEnum.docID())) {
            nc++;
          }
        }

        int docFreq = xc+nc;

        double entropyContainsTerm = binaryEntropy( (double) xc / docFreq );
        double entropyNotContainsTerm = binaryEntropy( (double) (numPositiveDocs - xc) / (numDocs - docFreq + 1) );
        double score = entropyC - ( (docFreq / numDocs) * entropyContainsTerm + (1.0 - docFreq / numDocs) * entropyNotContainsTerm);

        topFreq.add(term.utf8ToString(), docFreq);
        if (topTerms.size() < numTerms) {
          topTerms.add(new TermWithScore(term.utf8ToString(), score));
        } else  {
          if (topTerms.first().score < score) {
            topTerms.pollFirst();
            topTerms.add(new TermWithScore(term.utf8ToString(), score));
          }
        }
      }

      for (TermWithScore topTerm : topTerms) {
        analytics.add(topTerm.term, topTerm.score);
        topFreq.add(topTerm.term, allFreq.get(topTerm.term));
      }

      if (this.delegate instanceof DelegatingCollector) {
        ((DelegatingCollector) this.delegate).finish();
      }
    }

    private double binaryEntropy(double prob) {
      if (prob == 0 || prob == 1) return 0;
      return (-1 * prob * Math.log(prob)) + (-1 * (1.0 - prob) * Math.log(1.0 - prob));
    }

  }



  private static class TermWithScore implements Comparable<TermWithScore>{
    public final String term;
    public final double score;

    public TermWithScore(String term, double score) {
      this.term = term;
      this.score = score;
    }

    @Override
    public int hashCode() {
      return term.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) return false;
      if (obj.getClass() != getClass()) return false;
      TermWithScore other = (TermWithScore) obj;
      return other.term.equals(this.term);
    }

    @Override
    public int compareTo(TermWithScore o) {
      int cmp = Double.compare(this.score, o.score);
      if (cmp == 0) {
        return this.term.compareTo(o.term);
      } else {
        return cmp;
      }
    }
  }
}


