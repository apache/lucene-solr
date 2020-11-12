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

package org.apache.solr.ltr.interleaving;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.search.Query;
import org.apache.solr.ltr.search.LTRQuery;
import org.apache.solr.search.RankQuery;

/**
 * A learning to rank Query with Interleaving, will incapsulate two models, and delegate to it the rescoring
 * of the documents.
 **/
public class LTRInterleavingQuery extends LTRQuery {
  private final LTRInterleavingScoringQuery[] rerankingQueries;
  private final Interleaving interlavingAlgorithm;

  public LTRInterleavingQuery(Interleaving interleavingAlgorithm, LTRInterleavingScoringQuery[] rerankingQueries, int rerankDocs) {
    super(null, rerankDocs, new LTRInterleavingRescorer(interleavingAlgorithm, rerankingQueries));
    this.rerankingQueries = rerankingQueries;
    this.interlavingAlgorithm = interleavingAlgorithm;
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + (mainQuery.hashCode() + rerankingQueries.hashCode() + reRankDocs);
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o) && equalsTo(getClass().cast(o));
  }

  private boolean equalsTo(LTRInterleavingQuery other) {
    return (mainQuery.equals(other.mainQuery)
        && rerankingQueries.equals(other.rerankingQueries) && (reRankDocs == other.reRankDocs));
  }

  @Override
  public RankQuery wrap(Query _mainQuery) {
    super.wrap(_mainQuery);
    for(LTRInterleavingScoringQuery rerankingQuery: rerankingQueries){
      rerankingQuery.setOriginalQuery(_mainQuery);
    }
    return this;
  }

  @Override
  public String toString(String field) {
    return "{!ltr mainQuery='" + mainQuery.toString() + "' rerankingQueries='"
        + Arrays.toString(rerankingQueries) + "' reRankDocs=" + reRankDocs + "}";
  }

  @Override
  protected Query rewrite(Query rewrittenMainQuery) throws IOException {
    return new LTRInterleavingQuery(interlavingAlgorithm, rerankingQueries, reRankDocs).wrap(rewrittenMainQuery);
  }
}
