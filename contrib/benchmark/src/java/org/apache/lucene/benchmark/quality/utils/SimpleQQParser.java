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
package org.apache.lucene.benchmark.quality.utils;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.benchmark.quality.QualityQuery;
import org.apache.lucene.benchmark.quality.QualityQueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;

/**
 * Simplistic quality query parser. A Lucene query is created by passing 
 * the value of the specified QualityQuery name-value pair into 
 * a Lucene's QueryParser using StandardAnalyzer. */
public class SimpleQQParser implements QualityQueryParser {

  private String qqName;
  private String indexField;
  ThreadLocal queryParser = new ThreadLocal();

  /**
   * Constructor of a simple qq parser.
   * @param qqName name-value pair of quality query to use for creating the query
   * @param indexField corresponding index field  
   */
  public SimpleQQParser(String qqName, String indexField) {
    this.qqName = qqName;
    this.indexField = indexField;
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.benchmark.quality.QualityQueryParser#parse(org.apache.lucene.benchmark.quality.QualityQuery)
   */
  public Query parse(QualityQuery qq) throws ParseException {
    QueryParser qp = (QueryParser) queryParser.get();
    if (qp==null) {
      qp = new QueryParser(indexField, new StandardAnalyzer());
      queryParser.set(qp);
    }
    return qp.parse(qq.getValue(qqName));
  }

}
