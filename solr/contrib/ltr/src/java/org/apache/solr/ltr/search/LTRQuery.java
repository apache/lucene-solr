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

package org.apache.solr.ltr.search;

import java.io.IOException;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.ltr.LTRRescorer;
import org.apache.solr.ltr.LTRScoringQuery;
import org.apache.solr.search.AbstractReRankQuery;
import org.apache.solr.search.RankQuery;

/**
 * A learning to rank Query, will incapsulate a learning to rank model, and delegate to it the rescoring
 * of the documents.
 **/
public class LTRQuery extends AbstractReRankQuery {
  private static final Query defaultQuery = new MatchAllDocsQuery();
  private final LTRScoringQuery scoringQuery;

  public LTRQuery(LTRScoringQuery scoringQuery, int reRankDocs) {
    this(scoringQuery, reRankDocs, new LTRRescorer(scoringQuery));
  }

  protected LTRQuery(LTRScoringQuery scoringQuery, int reRankDocs, LTRRescorer rescorer) {
    super(defaultQuery, reRankDocs, rescorer);
    this.scoringQuery = scoringQuery;
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + (mainQuery.hashCode() + scoringQuery.hashCode() + reRankDocs);
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o) && equalsTo(getClass().cast(o));
  }

  private boolean equalsTo(LTRQuery other) {
    return (mainQuery.equals(other.mainQuery)
        && scoringQuery.equals(other.scoringQuery) && (reRankDocs == other.reRankDocs));
  }

  @Override
  public RankQuery wrap(Query _mainQuery) {
    super.wrap(_mainQuery);
    if (scoringQuery != null) {
      scoringQuery.setOriginalQuery(_mainQuery);
    }
    return this;
  }

  @Override
  public String toString(String field) {
    return "{!ltr mainQuery='" + mainQuery.toString() + "' scoringQuery='"
        + scoringQuery.toString() + "' reRankDocs=" + reRankDocs + "}";
  }

  @Override
  protected Query rewrite(Query rewrittenMainQuery) throws IOException {
    return new LTRQuery(scoringQuery, reRankDocs).wrap(rewrittenMainQuery);
  }
}
