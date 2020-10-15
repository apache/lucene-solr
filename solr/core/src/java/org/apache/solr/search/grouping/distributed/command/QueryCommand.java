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
package org.apache.solr.search.grouping.distributed.command;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.MaxScoreCollector;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.grouping.Command;
import org.apache.solr.search.grouping.collector.FilterCollector;

/**
 *
 */
public class QueryCommand implements Command<QueryCommandResult> {

  public static class Builder {

    private Sort sort;
    private String queryString;
    private Query query;
    private Query mainQuery;
    private DocSet docSet;
    private Integer docsToCollect;
    private boolean needScores;

    public Builder setSort(Sort sort) {
      this.sort = sort;
      return this;
    }

    /**
     * Sets the group query.
     *
     * @param query The {@link Query} used for grouping
     * @return this
     */
    public Builder setQuery(Query query) {
      this.query = query;
      return this;
    }

    /**
     * Sets the main query used for fetching results. This is mainly used for computing the scores.
     *
     * @param mainQuery The top-level query
     * @return this
     */
    public Builder setMainQuery(Query mainQuery) {
      this.mainQuery = mainQuery;
      return this;
    }

    /**
     * Sets the group query from the specified groupQueryString.
     * The groupQueryString is parsed into a query.
     *
     * @param groupQueryString The group query string to parse
     * @param request The current request
     * @return this
     */
    public Builder setQuery(String groupQueryString, SolrQueryRequest request) throws SyntaxError {
      QParser parser = QParser.getParser(groupQueryString, request);
      this.queryString = groupQueryString;
      return setQuery(parser.getQuery());
    }

    public Builder setDocSet(DocSet docSet) {
      this.docSet = docSet;
      return this;
    }

    /**
     * Sets the docSet based on the created {@link DocSet}
     *
     * @param searcher The searcher executing the
     * @return this
     * @throws IOException If I/O related errors occur.
     */
    public Builder setDocSet(SolrIndexSearcher searcher) throws IOException {
      return setDocSet(searcher.getDocSet(query));
    }

    public Builder setDocsToCollect(int docsToCollect) {
      this.docsToCollect = docsToCollect;
      return this;
    }

    public Builder setNeedScores(boolean needScores) {
      this.needScores = needScores;
      return this;
    }

    public QueryCommand build() {
      if (sort == null || query == null || docSet == null || docsToCollect == null || mainQuery == null) {
        throw new IllegalStateException("All fields must be set");
      }

      return new QueryCommand(sort, query, docsToCollect, needScores, docSet, queryString, mainQuery);
    }

  }

  private final Sort sort;
  private final Query query;
  private final DocSet docSet;
  private final int docsToCollect;
  private final boolean needScores;
  private final String queryString;
  private final Query mainQuery;

  @SuppressWarnings({"rawtypes"})
  private TopDocsCollector topDocsCollector;
  private FilterCollector filterCollector;
  private MaxScoreCollector maxScoreCollector;
  private TopDocs topDocs;

  private QueryCommand(Sort sort,
                       Query query,
                       int docsToCollect,
                       boolean needScores,
                       DocSet docSet,
                       String queryString,
                       Query mainQuery) {
    this.sort = sort;
    this.query = query;
    this.docsToCollect = docsToCollect;
    this.needScores = needScores;
    this.docSet = docSet;
    this.queryString = queryString;
    this.mainQuery = mainQuery;
  }

  @Override
  public List<Collector> create() throws IOException {
    Collector subCollector;
    if (sort == null || sort.equals(Sort.RELEVANCE)) {
      subCollector = topDocsCollector = TopScoreDocCollector.create(docsToCollect, Integer.MAX_VALUE);
    } else {
      topDocsCollector = TopFieldCollector.create(sort, docsToCollect, Integer.MAX_VALUE);
      if (needScores) {
        maxScoreCollector = new MaxScoreCollector();
        subCollector = MultiCollector.wrap(topDocsCollector, maxScoreCollector);
      } else {
        subCollector = topDocsCollector;
      }
    }
    filterCollector = new FilterCollector(docSet, subCollector);
    return Arrays.asList((Collector) filterCollector);
  }

  @Override
  public void postCollect(IndexSearcher searcher) throws IOException {
    topDocs = topDocsCollector.topDocs();
    if (needScores) {
      // use mainQuery to populate the scores
      TopFieldCollector.populateScores(topDocs.scoreDocs, searcher, mainQuery);
    }
  }

  @Override
  public QueryCommandResult result() throws IOException {
    float maxScore;
    if (sort == null) {
      maxScore = topDocs.scoreDocs.length == 0 ? Float.NaN : topDocs.scoreDocs[0].score;
    } else {
      maxScore = maxScoreCollector == null ? Float.NaN : maxScoreCollector.getMaxScore();
    }
    return new QueryCommandResult(topDocs, filterCollector.getMatches(), maxScore);
  }

  @Override
  public String getKey() {
    return queryString != null ? queryString : query.toString();
  }

  @Override
  public Sort getGroupSort() {
    return sort;
  }

  @Override
  public Sort getWithinGroupSort() {
    return null;
  }
}
