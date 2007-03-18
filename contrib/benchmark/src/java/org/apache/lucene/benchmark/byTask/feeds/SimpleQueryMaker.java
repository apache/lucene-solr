package org.apache.lucene.benchmark.byTask.feeds;

/**
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

import java.util.ArrayList;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.benchmark.byTask.utils.Format;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;

/**
 * A QueryMaker that makes queries for a collection created 
 * using {@link org.apache.lucene.benchmark.byTask.feeds.SimpleDocMaker}.
 */
public class SimpleQueryMaker implements QueryMaker {

  private int qnum = 0;
  private Query queries[];
  private Config config;
  
  /**
   * Prepare the queries for this test.
   * Extending classes can overide this method for preparing different queries. 
   * @return prepared queries.
   * @throws Exception if canot prepare the queries.
   */
  protected Query[] prepareQueries() throws Exception {
    // analyzer (default is standard analyzer)
    Analyzer anlzr= (Analyzer) Class.forName(config.get("analyzer",
        "org.apache.lucene.analysis.standard.StandardAnalyzer")).newInstance(); 
    
    QueryParser qp = new QueryParser("body",anlzr);
    ArrayList qq = new ArrayList();
    Query q1 = new TermQuery(new Term("docid","doc2"));
    qq.add(q1);
    Query q2 = new TermQuery(new Term("body","simple"));
    qq.add(q2);
    BooleanQuery bq = new BooleanQuery();
    bq.add(q1,Occur.MUST);
    bq.add(q2,Occur.MUST);
    qq.add(bq);
    qq.add(qp.parse("synthetic body"));
    qq.add(qp.parse("\"synthetic body\""));
    qq.add(qp.parse("synthetic text"));
    qq.add(qp.parse("\"synthetic text\""));
    qq.add(qp.parse("\"synthetic text\"~3"));
    qq.add(qp.parse("zoom*"));
    qq.add(qp.parse("synth*"));
    return (Query []) qq.toArray(new Query[0]);
  }

  public Query makeQuery() throws Exception {
    return queries[nextQnum()];
  }

  public void setConfig(Config config) throws Exception {
    this.config = config;
    queries = prepareQueries();
  }

  public void resetInputs() {
    qnum = 0;
  }

  // return next qnum
  private synchronized int nextQnum() {
    int res = qnum;
    qnum = (qnum+1) % queries.length;
    return res;
  }

  public String printQueries() {
    String newline = System.getProperty("line.separator");
    StringBuffer sb = new StringBuffer();
    if (queries != null) {
      for (int i = 0; i < queries.length; i++) {
        sb.append(i+". "+Format.simpleName(queries[i].getClass())+" - "+queries[i].toString());
        sb.append(newline);
      }
    }
    return sb.toString();
  }

  /*
   *  (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.feeds.QueryMaker#makeQuery(int)
   */
  public Query makeQuery(int size) throws Exception {
    throw new Exception(this+".makeQuery(int size) is not supported!");
  }

}
