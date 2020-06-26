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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryUtils;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;

/**
 * This feature allows you to reuse any Solr query as a feature. The value
 * of the feature will be the score of the given query for the current document.
 * See <a href="https://lucene.apache.org/solr/guide/other-parsers.html">Solr documentation of other parsers</a> you can use as a feature.
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

  // The setters will be invoked via reflection from the passed in params

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
    final LinkedHashMap<String,Object> params = defaultParamsToMap();
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
    return new SolrFeatureWeight((SolrIndexSearcher) searcher, request, originalQuery, efi);
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
    private final Weight solrQueryWeight;

    public SolrFeatureWeight(SolrIndexSearcher searcher,
                             SolrQueryRequest request, Query originalQuery, Map<String, String[]> efi) throws IOException {
      super(SolrFeature.this, searcher, request, originalQuery, efi);
      try {
        final SolrQueryRequest req = makeRequest(request.getCore(), q, fq, df);
        if (req == null) {
          throw new IOException("ERROR: No parameters provided");
        }

        // Build the scoring query
        Query scoreQuery;
        String qStr = q;
        if (qStr == null || qStr.isEmpty()) {
          scoreQuery = null; // ultimately behaves like MatchAllDocsQuery
        } else {
          qStr = macroExpander.expand(qStr);
          if (qStr == null) {
            throw new FeatureException(this.getClass().getSimpleName() + " requires efi parameter that was not passed in request.");
          }
          scoreQuery = QParser.getParser(qStr, req).getQuery();
          // note: QParser can return a null Query sometimes, such as if the query is a stopword or just symbols
          if (scoreQuery == null) {
            scoreQuery = new MatchNoDocsQuery(); // debatable; all or none?
          }
        }

        // Build the filter queries
        Query filterDocSetQuery = null;
        if (fq != null) {
          List<Query> filterQueries = new ArrayList<>(); // If there are no fqs we just want an empty list
          for (String fqStr : fq) {
            if (fqStr != null) {
              fqStr = macroExpander.expand(fqStr);
              if (fqStr == null) {
                throw new FeatureException(this.getClass().getSimpleName() + " requires efi parameter that was not passed in request.");
              }
              final Query filterQuery = QParser.getParser(fqStr, req).getQuery();
              if (filterQuery != null) {
                filterQueries.add(filterQuery);
              }
            }
          }

          DocSet filtersDocSet = searcher.getDocSet(filterQueries); // execute
          if (filtersDocSet != searcher.getLiveDocSet()) {
            filterDocSetQuery = filtersDocSet.getTopFilter();
          }
        }

        Query query = QueryUtils.combineQueryAndFilter(scoreQuery, filterDocSetQuery);

        solrQueryWeight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE, 1);

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
      Scorer solrScorer = solrQueryWeight.scorer(context);
      if (solrScorer == null) {
        return null;
      }
      return new SolrFeatureScorer(this, solrScorer);
    }

    /**
     * Scorer for a SolrFeature
     */
    public class SolrFeatureScorer extends FilterFeatureScorer {

      public SolrFeatureScorer(FeatureWeight weight, Scorer solrScorer) {
        super(weight, solrScorer);
      }

      @Override
      public float score() throws IOException {
        try {
          return in.score();
        } catch (UnsupportedOperationException e) {
          throw new FeatureException(
              e.toString() + ": " +
                  "Unable to extract feature for "
                  + name, e);
        }
      }

    }
  }
}