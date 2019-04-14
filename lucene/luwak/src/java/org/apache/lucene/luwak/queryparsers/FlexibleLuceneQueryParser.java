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

package org.apache.lucene.luwak.queryparsers;

import java.util.HashMap;


import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.search.Query;
import org.apache.lucene.luwak.MonitorQueryParser;

/**
 * A query parser that uses the default Lucene parser
 */
public class FlexibleLuceneQueryParser implements MonitorQueryParser {

  private final String defaultField;
  private final Analyzer analyzer;
  private final Map<String, PointsConfig> pointsConfig;
  private final StandardQueryParser parser;

  /**
   * Creates a parser with a given default field and analyzer
   *
   * @param defaultField the default field
   * @param analyzer     an analyzer to use to analyzer query terms
   * @param pointsConfig configures pointfields like this https://github.com/apache/lucene-solr/blob/master/lucene/queryparser/src/test/org/apache/lucene/queryparser/flexible/standard/TestPointQueryParser.java
   */
  public FlexibleLuceneQueryParser(String defaultField, Analyzer analyzer, Map<String, PointsConfig> pointsConfig) {
    this.defaultField = defaultField;
    this.analyzer = analyzer;
    this.pointsConfig = pointsConfig;
    this.parser = new StandardQueryParser(this.analyzer);
  }

  /**
   * Creates a parser using a lucene {@link StandardAnalyzer}
   *
   * @param defaultField the default field
   * @param analyzer     an analyzer to use to analyzer query terms
   */
  public FlexibleLuceneQueryParser(String defaultField, Analyzer analyzer) {
    this(defaultField, analyzer, new HashMap<>());
  }

  /**
   * Creates a parser using a lucene {@link StandardAnalyzer}
   *
   * @param defaultField the default field
   */
  public FlexibleLuceneQueryParser(String defaultField) {
    this(defaultField, new StandardAnalyzer(), new HashMap<>());
  }

  /**
   * returns internal points config
   */
  public Map<String, PointsConfig> getPointsConfig() {
    return this.pointsConfig;
  }

  /**
   * return the StandardQueryParser
   */
  public StandardQueryParser getParser() {
    return this.parser;
  }

  @Override
  public Query parse(String query, Map<String, String> metadata) throws Exception {
    if (this.pointsConfig.size() > 0) {
      parser.setPointsConfigMap(pointsConfig);
    }
    return parser.parse(query, this.defaultField);
  }
}
