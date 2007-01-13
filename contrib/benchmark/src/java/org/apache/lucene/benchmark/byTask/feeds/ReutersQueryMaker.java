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
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.benchmark.byTask.utils.Format;


/**
 * A QueryMaker that makes queries devised manually (by Grant Ingersoll) for
 * searching in the Reuters collection.
 */
public class ReutersQueryMaker implements QueryMaker {
  
  private int qnum = 0;
  private Query queries[];
  private Config config;
  
  private static String [] STANDARD_QUERIES = {
    //Start with some short queries
    "Salomon", "Comex", "night trading", "Japan Sony",
    //Try some Phrase Queries
    "\"Sony Japan\"", "\"food needs\"~3",
    "\"World Bank\"^2 AND Nigeria", "\"World Bank\" -Nigeria",
    "\"Ford Credit\"~5",
    //Try some longer queries
    "airline Europe Canada destination",
    "Long term pressure by trade " +
    "ministers is necessary if the current Uruguay round of talks on " +
    "the General Agreement on Trade and Tariffs (GATT) is to " +
    "succeed"
  };
  
  private static Query[] getPrebuiltQueries(String field) {
    //  be wary of unanalyzed text
    return new Query[] {
        new SpanFirstQuery(new SpanTermQuery(new Term(field, "ford")), 5),
        new SpanNearQuery(new SpanQuery[]{new SpanTermQuery(new Term(field, "night")), new SpanTermQuery(new Term(field, "trading"))}, 4, false),
        new SpanNearQuery(new SpanQuery[]{new SpanFirstQuery(new SpanTermQuery(new Term(field, "ford")), 10), new SpanTermQuery(new Term(field, "credit"))}, 10, false),
        new WildcardQuery(new Term(field, "fo*")),
    };
  }
  
  /**
   * Parse the strings containing Lucene queries.
   *
   * @param qs array of strings containing query expressions
   * @param a  analyzer to use when parsing queries
   * @return array of Lucene queries
   */
  private static Query[] createQueries(List qs, Analyzer a) {
    QueryParser qp = new QueryParser("body", a);
    List queries = new ArrayList();
    for (int i = 0; i < qs.size(); i++)  {
      try {
        
        Object query = qs.get(i);
        Query q = null;
        if (query instanceof String) {
          q = qp.parse((String) query);
          
        } else if (query instanceof Query) {
          q = (Query) query;
          
        } else {
          System.err.println("Unsupported Query Type: " + query);
        }
        
        if (q != null) {
          queries.add(q);
        }
        
      } catch (Exception e)  {
        e.printStackTrace();
      }
    }
    
    return (Query[]) queries.toArray(new Query[0]);
  }
  
  private void prepareQueries() throws Exception {
    // analyzer (default is standard analyzer)
    Analyzer anlzr= (Analyzer) Class.forName(config.get("analyzer",
    "org.apache.lucene.analysis.StandardAnalyzer")).newInstance(); 
    
    List queryList = new ArrayList(20);
    queryList.addAll(Arrays.asList(STANDARD_QUERIES));
    queryList.addAll(Arrays.asList(getPrebuiltQueries("body")));
    queries = createQueries(queryList, anlzr);
  }
  
  public Query makeQuery() throws Exception {
    return queries[nextQnum()];
  }
  
  public void setConfig(Config config) throws Exception {
    this.config = config;
    prepareQueries();
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
