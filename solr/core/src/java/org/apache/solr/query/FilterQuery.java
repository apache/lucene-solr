package org.apache.solr.query;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ToStringUtils;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.ExtendedQueryBase;
import org.apache.solr.search.SolrIndexSearcher;

public class FilterQuery extends ExtendedQueryBase {
  protected final Query q;

  public FilterQuery(Query q) {
    this.q = q;
    setBoost(0);  // default boost is 0;
  }

  public Query getQuery() {
    return q;
  }

  @Override
  public int hashCode() {
    return q.hashCode() + 0xc0e65615;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof FilterQuery)) return false;
    FilterQuery fq = (FilterQuery)obj;
    return this.q.equals(fq.q) && this.getBoost() == fq.getBoost();
  }

  @Override
  public String toString(String field) {
    StringBuilder sb = new StringBuilder();
    sb.append("field(");
    sb.append(q.toString(""));
    sb.append(')');
    sb.append(ToStringUtils.boost(getBoost()));
    return sb.toString();
  }


  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query newQ = q.rewrite(reader);
    if (newQ != q) {
      FilterQuery fq = new FilterQuery(newQ);
      fq.setBoost(this.getBoost());
      return fq;
    } else {
      return this;
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needScores) throws IOException {
    // SolrRequestInfo reqInfo = SolrRequestInfo.getRequestInfo();

    if (!(searcher instanceof SolrIndexSearcher)) {
      // delete-by-query won't have SolrIndexSearcher
      ConstantScoreQuery csq = new ConstantScoreQuery(q);
      csq.setBoost(this.getBoost());
      return csq.createWeight(searcher, needScores);
    }

    SolrIndexSearcher solrSearcher = (SolrIndexSearcher)searcher;
    DocSet docs = solrSearcher.getDocSet(q);
    // reqInfo.addCloseHook(docs);  // needed for off-heap refcounting

    ConstantScoreQuery csq = new ConstantScoreQuery( docs.getTopFilter() );
    csq.setBoost( this.getBoost() );
    return csq.createWeight(searcher, needScores);
  }
}
