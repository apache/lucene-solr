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
package org.apache.solr.ltr.feature;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
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
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
/**
 * This feature allows you to reuse any Solr query as a feature. The value
 * of the feature will be the score of the given query for the current document.
 * See <a href="https://cwiki.apache.org/confluence/display/solr/Other+Parsers">Solr documentation of other parsers</a> you can use as a feature.
 * Example configurations:
 * <pre>[{ "name": "isBook",
  "class": "org.apache.solr.ltr.feature.SolrFeature",
  "params":{ "fq": ["{!terms f=category}book"] }
},
{
  "name":  "documentRecency",
  "class": "org.apache.solr.ltr.feature.SolrFeature",
  "params": {
      "q": "{!func}recip( ms(NOW,publish_date), 3.16e-11, 1, 1)"
  }
}]</pre>
 **/
public class SolrFeature extends Feature {

  private String df;
  private String q;
  private List<String> fq;

  public String getDf() {
    return df;
  }

  public void setDf(String df) {
    this.df = df;
  }

  public String getQ() {
    return q;
  }

  public void setQ(String q) {
    this.q = q;
  }

  public List<String> getFq() {
    return fq;
  }

  public void setFq(List<String> fq) {
    this.fq = fq;
  }

  public SolrFeature(String name, Map<String,Object> params) {
    super(name, params);
  }

  @Override
  public LinkedHashMap<String,Object> paramsToMap() {
    final LinkedHashMap<String,Object> params = new LinkedHashMap<>(3, 1.0f);
    if (df != null) {
      params.put("df", df);
    }
    if (q != null) {
      params.put("q", q);
    }
    if (fq != null) {
      params.put("fq", fq);
    }
    return params;
  }

  @Override
  public FeatureWeight createWeight(IndexSearcher searcher, boolean needsScores,
      SolrQueryRequest request, Query originalQuery, Map<String,String[]> efi)
          throws IOException {
    return new SolrFeatureWeight(searcher, request, originalQuery, efi);
  }

  @Override
  protected void validate() throws FeatureException {
    if ((q == null || q.isEmpty()) &&
        ((fq == null) || fq.isEmpty())) {
      throw new FeatureException(getClass().getSimpleName()+
          ": Q or FQ must be provided");
    }
  }
  /**
   * Weight for a SolrFeature
   **/
  public class SolrFeatureWeight extends FeatureWeight {
    final private Weight solrQueryWeight;
    final private Query query;
    final private List<Query> queryAndFilters;

    public SolrFeatureWeight(IndexSearcher searcher,
        SolrQueryRequest request, Query originalQuery, Map<String,String[]> efi) throws IOException {
      super(SolrFeature.this, searcher, request, originalQuery, efi);
      try {
        String solrQuery = q;
        final List<String> fqs = fq;

        if ((solrQuery == null) || solrQuery.isEmpty()) {
          solrQuery = "*:*";
        }

        solrQuery = macroExpander.expand(solrQuery);
        if (solrQuery == null) {
          throw new FeatureException(this.getClass().getSimpleName()+" requires efi parameter that was not passed in request.");
        }

        final SolrQueryRequest req = makeRequest(request.getCore(), solrQuery,
            fqs, df);
        if (req == null) {
          throw new IOException("ERROR: No parameters provided");
        }

        // Build the filter queries
        queryAndFilters = new ArrayList<Query>(); // If there are no fqs we just want an empty list
        if (fqs != null) {
          for (String fq : fqs) {
            if ((fq != null) && (fq.trim().length() != 0)) {
              fq = macroExpander.expand(fq);
              final QParser fqp = QParser.getParser(fq, req);
              final Query filterQuery = fqp.getQuery();
              if (filterQuery != null) {
                queryAndFilters.add(filterQuery);
              }
            }
          }
        }

        final QParser parser = QParser.getParser(solrQuery, req);
        query = parser.parse();

        // Query can be null if there was no input to parse, for instance if you
        // make a phrase query with "to be", and the analyzer removes all the
        // words
        // leaving nothing for the phrase query to parse.
        if (query != null) {
          queryAndFilters.add(query);
          solrQueryWeight = searcher.createNormalizedWeight(query, true);
        } else {
          solrQueryWeight = null;
        }
      } catch (final SyntaxError e) {
        throw new FeatureException("Failed to parse feature query.", e);
      }
    }

    private LocalSolrQueryRequest makeRequest(SolrCore core, String solrQuery,
        List<String> fqs, String df) {
      final NamedList<String> returnList = new NamedList<String>();
      if ((solrQuery != null) && !solrQuery.isEmpty()) {
        returnList.add(CommonParams.Q, solrQuery);
      }
      if (fqs != null) {
        for (final String fq : fqs) {
          returnList.add(CommonParams.FQ, fq);
        }
      }
      if ((df != null) && !df.isEmpty()) {
        returnList.add(CommonParams.DF, df);
      }
      if (returnList.size() > 0) {
        return new LocalSolrQueryRequest(core, returnList);
      } else {
        return null;
      }
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      if (solrQueryWeight != null) {
        solrQueryWeight.extractTerms(terms);
      }
    }

    @Override
    public FeatureScorer scorer(LeafReaderContext context) throws IOException {
      Scorer solrScorer = null;
      if (solrQueryWeight != null) {
        solrScorer = solrQueryWeight.scorer(context);
      }

      final DocIdSetIterator idItr = getDocIdSetIteratorFromQueries(
          queryAndFilters, context);
      if (idItr != null) {
        return solrScorer == null ? new ValueFeatureScorer(this, 1f, idItr)
            : new SolrFeatureScorer(this, solrScorer,
                new SolrFeatureScorerIterator(idItr, solrScorer.iterator()));
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
    private DocIdSetIterator getDocIdSetIteratorFromQueries(List<Query> queries,
        LeafReaderContext context) throws IOException {
      final SolrIndexSearcher.ProcessedFilter pf = ((SolrIndexSearcher) searcher)
          .getProcessedFilter(null, queries);
      final Bits liveDocs = context.reader().getLiveDocs();

      DocIdSetIterator idIter = null;
      if (pf.filter != null) {
        final DocIdSet idSet = pf.filter.getDocIdSet(context, liveDocs);
        if (idSet != null) {
          idIter = idSet.iterator();
        }
      }

      return idIter;
    }

    /**
     * Scorer for a SolrFeature
     **/
    public class SolrFeatureScorer extends FeatureScorer {
      final private Scorer solrScorer;

      public SolrFeatureScorer(FeatureWeight weight, Scorer solrScorer,
          SolrFeatureScorerIterator itr) {
        super(weight, itr);
        this.solrScorer = solrScorer;
      }

      @Override
      public float score() throws IOException {
        try {
          return solrScorer.score();
        } catch (UnsupportedOperationException e) {
          throw new FeatureException(
              e.toString() + ": " +
                  "Unable to extract feature for "
                  + name, e);
        }
      }
    }

    /**
     * An iterator that allows to iterate only on the documents for which a feature has
     * a value.
     **/
    public class SolrFeatureScorerIterator extends DocIdSetIterator {

      final private DocIdSetIterator filterIterator;
      final private DocIdSetIterator scorerFilter;

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
        int docID = filterIterator.nextDoc();
        scorerFilter.advance(docID);
        return docID;
      }

      @Override
      public int advance(int target) throws IOException {
        // We use iterator to catch the scorer up since
        // that checks if the target id is in the query + all the filters
        int docID = filterIterator.advance(target);
        scorerFilter.advance(docID);
        return docID;
      }

      @Override
      public long cost() {
        return filterIterator.cost() + scorerFilter.cost();
      }

    }
  }

}
