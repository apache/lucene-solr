package org.apache.solr.ltr.feature.impl;

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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.ltr.feature.norm.Normalizer;
import org.apache.solr.ltr.ranking.Feature;
import org.apache.solr.ltr.ranking.FeatureScorer;
import org.apache.solr.ltr.ranking.FeatureWeight;
import org.apache.solr.ltr.util.FeatureException;
import org.apache.solr.ltr.util.NamedParams;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrIndexSearcher.ProcessedFilter;

public class SolrFeature extends Feature {

  @Override
  public FeatureWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    return new SolrFeatureWeight(searcher, name, params, norm, id);
  }

  public class SolrFeatureWeight extends FeatureWeight {
    Weight solrQueryWeight;
    Query query;
    List<Query> queryAndFilters;

    public SolrFeatureWeight(IndexSearcher searcher, String name,
        NamedParams params, Normalizer norm, int id) throws IOException {
      super(SolrFeature.this, searcher, name, params, norm, id);
    }

    @Override
    public void process() throws FeatureException {
      try {
        String df = (String) getParams().get(CommonParams.DF);
        String defaultParser = (String) getParams().get("defaultParser");
        String solrQuery = (String) getParams().get(CommonParams.Q);
        List<String> fqs = (List<String>) getParams().get(CommonParams.FQ);

        if ((solrQuery == null || solrQuery.isEmpty())
            && (fqs == null || fqs.isEmpty())) {
          throw new IOException("ERROR: FQ or Q have not been provided");
        }

        if (solrQuery == null || solrQuery.isEmpty()) {
          solrQuery = "*:*";
        }
        solrQuery = macroExpander.expand(solrQuery);

        SolrQueryRequest req = makeRequest(request.getCore(), solrQuery, fqs,
            df);
        if (req == null) {
          throw new IOException("ERROR: No parameters provided");
        }

        // Build the filter queries
        this.queryAndFilters = new ArrayList<Query>(); // If there are no fqs we
                                                       // just want an empty
                                                       // list
        if (fqs != null) {
          for (String fq : fqs) {
            if (fq != null && fq.trim().length() != 0) {
              fq = macroExpander.expand(fq);
              QParser fqp = QParser.getParser(fq, null, req);
              Query filterQuery = fqp.getQuery();
              if (filterQuery != null) {
                queryAndFilters.add(filterQuery);
              }
            }
          }
        }

        QParser parser = QParser.getParser(solrQuery,
            defaultParser == null ? "lucene" : defaultParser, req);
        query = parser.parse();

        // Query can be null if there was no input to parse, for instance if you
        // make a phrase query with "to be", and the analyzer removes all the
        // words
        // leaving nothing for the phrase query to parse.
        if (query != null) {
          queryAndFilters.add(query);
          solrQueryWeight = searcher.createNormalizedWeight(query, true);
        }

      } catch (Exception e) {
        throw new FeatureException("Exception for " + this.toString() + " "
            + e.getMessage(), e);
      }
    }

    private LocalSolrQueryRequest makeRequest(SolrCore core, String solrQuery,
        List<String> fqs, String df) {
      // Map.Entry<String, String> [] entries = new NamedListEntry[q.length /
      // 2];
      NamedList<String> returnList = new NamedList<String>();
      if (solrQuery != null && !solrQuery.isEmpty()) {
        returnList.add(CommonParams.Q, solrQuery);
      }
      if (fqs != null) {
        for (String fq : fqs) {
          returnList.add(CommonParams.FQ, fq);
          // entries[i/2] = new NamedListEntry<>(q[i], q[i+1]);
        }
      }
      if (df != null && !df.isEmpty()) {
        returnList.add(CommonParams.DF, df);
      }
      if (returnList.size() > 0) return new LocalSolrQueryRequest(core,
          returnList);
      else return null;
    }

    @Override
    public FeatureScorer scorer(LeafReaderContext context) throws IOException {
      Scorer solrScorer = null;
      if (solrQueryWeight != null) {
        solrScorer = solrQueryWeight.scorer(context);
      }

      DocIdSetIterator idItr = getDocIdSetIteratorFromQueries(queryAndFilters,
          context);
      if (idItr != null) {
        return solrScorer == null ? new SolrFeatureFilterOnlyScorer(this, idItr)
            : new SolrFeatureScorer(this, solrScorer, idItr);
      } else {
        return null;
      }
    }

    /**
     * Given a list of Solr filters/queries, return a doc iterator that
     * traverses over the documents that matched all the criteria of the
     * queries.
     *
     * @param queries
     *          Filtering criteria to match documents against
     * @param context
     *          Index reader
     * @return DocIdSetIterator to traverse documents that matched all filter
     *         criteria
     */
    public DocIdSetIterator getDocIdSetIteratorFromQueries(List<Query> queries,
        LeafReaderContext context) throws IOException {
      // FIXME: Only SolrIndexSearcher has getProcessedFilter(), but all weights
      // are given an IndexSearcher instead.
      // Ideally there should be some guarantee that we have a SolrIndexSearcher
      // so we don't have to cast.
      ProcessedFilter pf = ((SolrIndexSearcher) searcher).getProcessedFilter(
          null, queries);
      final Bits liveDocs = context.reader().getLiveDocs();

      DocIdSetIterator idIter = null;
      if (pf.filter != null) {
        DocIdSet idSet = pf.filter.getDocIdSet(context, liveDocs);
        if (idSet != null) idIter = idSet.iterator();
      }

      return idIter;
    }

    public class SolrFeatureScorer extends FeatureScorer {
      Scorer solrScorer;
      String q;
      DocIdSetIterator itr;

      public SolrFeatureScorer(FeatureWeight weight, Scorer solrScorer,
          DocIdSetIterator filterIterator) {
        super(weight);
        q = (String) getParams().get(CommonParams.Q);
        this.solrScorer = solrScorer;
        this.itr = new SolrFeatureScorerIterator(filterIterator,
            solrScorer.iterator());
      }

      @Override
      public float score() throws IOException {
        return solrScorer.score();
      }

      @Override
      public String toString() {
        return "SolrFeature [function:" + q + "]";
      }

      @Override
      public DocIdSetIterator iterator() {
        return itr;
      }

      @Override
      public int docID() {
        return itr.docID();
      }

      private class SolrFeatureScorerIterator extends DocIdSetIterator {

        DocIdSetIterator filterIterator;
        DocIdSetIterator scorerFilter;
        int docID;

        SolrFeatureScorerIterator(DocIdSetIterator filterIterator,
            DocIdSetIterator scorerFilter) {
          this.filterIterator = filterIterator;
          this.scorerFilter = scorerFilter;
        }

        @Override
        public int docID() {
          return filterIterator.docID();
        }

        @Override
        public int nextDoc() throws IOException {
          docID = filterIterator.nextDoc();
          scorerFilter.advance(docID);
          return docID;
        }

        @Override
        public int advance(int target) throws IOException {
          // We use iterator to catch the scorer up since
          // that checks if the target id is in the query + all the filters
          docID = filterIterator.advance(target);
          scorerFilter.advance(docID);
          return docID;
        }

        @Override
        public long cost() {
          return 0; // FIXME: Make this work?
        }

      }
    }

    public class SolrFeatureFilterOnlyScorer extends FeatureScorer {
      String fq;
      DocIdSetIterator itr;

      public SolrFeatureFilterOnlyScorer(FeatureWeight weight,
          DocIdSetIterator iterator) {
        super(weight);
        fq = (String) getParams().get(CommonParams.FQ);
        this.itr = iterator;
      }

      @Override
      public float score() throws IOException {
        return 1f;
      }

      @Override
      public String toString() {
        return "SolrFeature [function:" + fq + "]";
      }

      @Override
      public DocIdSetIterator iterator() {
        return itr;
      }

      @Override
      public int docID() {
        return itr.docID();
      }

    }

  }

}
