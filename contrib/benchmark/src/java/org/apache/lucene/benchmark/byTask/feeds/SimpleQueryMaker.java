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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.util.ArrayList;

/**
 * A QueryMaker that makes queries for a collection created 
 * using {@link org.apache.lucene.benchmark.byTask.feeds.SimpleDocMaker}.
 */
public class SimpleQueryMaker extends AbstractQueryMaker implements QueryMaker {


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
    
    QueryParser qp = new QueryParser(BasicDocMaker.BODY_FIELD,anlzr);
    ArrayList qq = new ArrayList();
    Query q1 = new TermQuery(new Term(BasicDocMaker.ID_FIELD,"doc2"));
    qq.add(q1);
    Query q2 = new TermQuery(new Term(BasicDocMaker.BODY_FIELD,"simple"));
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

}
