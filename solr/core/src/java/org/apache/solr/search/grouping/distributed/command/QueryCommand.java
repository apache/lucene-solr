package org.apache.solr.search.grouping.distributed.command;

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

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.*;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.grouping.Command;
import org.apache.solr.search.grouping.collector.FilterCollector;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class QueryCommand implements Command<QueryCommandResult> {

  public static class Builder {

    private Sort sort;
    private String queryString;
    private Query query;
    private DocSet docSet;
    private Integer docsToCollect;
    private boolean needScores;

    public Builder setSort(Sort sort) {
      this.sort = sort;
      return this;
    }

    public Builder setQuery(Query query) {
      this.query = query;
      return this;
    }

    /**
     * Sets the group query from the specified groupQueryString.
     * The groupQueryString is parsed into a query.
     *
     * @param groupQueryString The group query string to parse
     * @param request The current request
     * @return this
     * @throws ParseException If parsing the groupQueryString failed
     */
    public Builder setQuery(String groupQueryString, SolrQueryRequest request) throws ParseException {
      QParser parser = QParser.getParser(groupQueryString, null, request);
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
     * @return
     * @throws IOException
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
      if (sort == null || query == null || docSet == null || docsToCollect == null) {
        throw new IllegalStateException("All fields must be set");
      }

      return new QueryCommand(sort, query, docsToCollect, needScores, docSet, queryString);
    }

  }

  private final Sort sort;
  private final Query query;
  private final DocSet docSet;
  private final int docsToCollect;
  private final boolean needScores;
  private final String queryString;

  private TopDocsCollector collector;
  private FilterCollector filterCollector;

  private QueryCommand(Sort sort, Query query, int docsToCollect, boolean needScores, DocSet docSet, String queryString) {
    this.sort = sort;
    this.query = query;
    this.docsToCollect = docsToCollect;
    this.needScores = needScores;
    this.docSet = docSet;
    this.queryString = queryString;
  }

  public List<Collector> create() throws IOException {
    if (sort == null || sort == Sort.RELEVANCE) {
      collector = TopScoreDocCollector.create(docsToCollect, true);
    } else {
      collector = TopFieldCollector.create(sort, docsToCollect, true, needScores, needScores, true);
    }
    filterCollector = new FilterCollector(docSet, collector);
    return Arrays.asList((Collector) filterCollector);
  }

  public QueryCommandResult result() {
    return new QueryCommandResult(collector.topDocs(), filterCollector.getMatches());
  }

  public String getKey() {
    return queryString != null ? queryString : query.toString();
  }

  public Sort getGroupSort() {
    return sort;
  }

  public Sort getSortWithinGroup() {
    return null;
  }
}
