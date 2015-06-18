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
package org.apache.lucene.benchmark.quality.utils;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.benchmark.quality.QualityQuery;
import org.apache.lucene.benchmark.quality.QualityQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

/**
 * Simplistic quality query parser. A Lucene query is created by passing 
 * the value of the specified QualityQuery name-value pair(s) into 
 * a Lucene's QueryParser using StandardAnalyzer. */
public class SimpleQQParser implements QualityQueryParser {

  private String qqNames[];
  private String indexField;
  ThreadLocal<QueryParser> queryParser = new ThreadLocal<>();

  /**
   * Constructor of a simple qq parser.
   * @param qqNames name-value pairs of quality query to use for creating the query
   * @param indexField corresponding index field  
   */
  public SimpleQQParser(String qqNames[], String indexField) {
    this.qqNames = qqNames;
    this.indexField = indexField;
  }

  /**
   * Constructor of a simple qq parser.
   * @param qqName name-value pair of quality query to use for creating the query
   * @param indexField corresponding index field  
   */
  public SimpleQQParser(String qqName, String indexField) {
    this(new String[] { qqName }, indexField);
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.benchmark.quality.QualityQueryParser#parse(org.apache.lucene.benchmark.quality.QualityQuery)
   */
  @Override
  public Query parse(QualityQuery qq) throws ParseException {
    QueryParser qp = queryParser.get();
    if (qp==null) {
      qp = new QueryParser(indexField, new StandardAnalyzer());
      queryParser.set(qp);
    }
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    for (int i = 0; i < qqNames.length; i++)
      bq.add(qp.parse(QueryParserBase.escape(qq.getValue(qqNames[i]))), BooleanClause.Occur.SHOULD);
    
    return bq.build();
  }

}
