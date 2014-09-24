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

package org.apache.solr.analytics.accumulator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.analytics.expression.Expression;
import org.apache.solr.analytics.expression.ExpressionFactory;
import org.apache.solr.analytics.request.AnalyticsRequest;
import org.apache.solr.analytics.request.ExpressionRequest;
import org.apache.solr.analytics.statistics.StatsCollector;
import org.apache.solr.analytics.statistics.StatsCollectorSupplierFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;

/**
 * A <code>BasicAccumulator</code> manages the ValueCounters and Expressions without regard to Facets.
 */
public class BasicAccumulator extends ValueAccumulator {
  private static final Logger log = LoggerFactory.getLogger(BasicAccumulator.class);
  protected final SolrIndexSearcher searcher;
  protected final AnalyticsRequest request;
  protected final DocSet docs;
  protected final Supplier<StatsCollector[]> statsCollectorArraySupplier;
  protected final StatsCollector[] statsCollectors;
  protected final Expression[] expressions;
  protected final String[] expressionNames;
  protected final String[] expressionStrings;
  protected final Set<String> hiddenExpressions;
  protected LeafReaderContext context = null;
  
  public BasicAccumulator(SolrIndexSearcher searcher, DocSet docs, AnalyticsRequest request) throws IOException {
    this.searcher = searcher;
    this.docs = docs;
    this.request = request;
    final List<ExpressionRequest> exRequests = new ArrayList<ExpressionRequest>(request.getExpressions()); // make a copy here
    Collections.sort(exRequests);
    log.info("Processing request '"+request.getName()+"'");
    statsCollectorArraySupplier = StatsCollectorSupplierFactory.create(searcher.getSchema(), exRequests);
    statsCollectors = statsCollectorArraySupplier.get();
    int size = exRequests.size();
    expressionNames = new String[size];
    expressionStrings = new String[size];
    int count = 0;
    for (ExpressionRequest expRequest : exRequests) {
      expressionNames[count] = expRequest.getName();
      expressionStrings[count++] = expRequest.getExpressionString();
    }
    expressions = makeExpressions(statsCollectors);
    hiddenExpressions = request.getHiddenExpressions();
  }
  
  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    this.context = context;
    for (StatsCollector counter : statsCollectors) {
      counter.setNextReader(context);
    }
  }
 
  public static BasicAccumulator create(SolrIndexSearcher searcher, DocSet docs, AnalyticsRequest request) throws IOException {
    return new BasicAccumulator(searcher,docs,request);
  }
  
  /**
   * Passes the documents on to the {@link StatsCollector}s to be collected.
   * @param doc Document to collect from
   */
  @Override
  public void collect(int doc) throws IOException {
    for (StatsCollector statsCollector : statsCollectors) {
      statsCollector.collect(doc);
    }
  }
  
  @Override
  public void compute() {
    for (StatsCollector statsCollector : statsCollectors) {
      statsCollector.compute();
    }
  }
  
  public NamedList<?> export(){
    NamedList<Object> base = new NamedList<>();
    for (int count = 0; count < expressions.length; count++) {
      if (!hiddenExpressions.contains(expressionNames[count])) {
        base.add(expressionNames[count], expressions[count].getValue());
      }
    }
    return base;
  }
  
  /**
   * Builds an array of Expressions with the given list of counters
   * @param statsCollectors the stats collectors
   * @return The array of Expressions
   */
  public Expression[] makeExpressions(StatsCollector[] statsCollectors) {
   Expression[] expressions = new Expression[expressionStrings.length];
    for (int count = 0; count < expressionStrings.length; count++) {
      expressions[count] = ExpressionFactory.create(expressionStrings[count], statsCollectors);
    }
    return expressions;
  }
  
  /**
   * Returns the value of an expression to use in a field or query facet.
   * @param expressionName the name of the expression
   * @return String String representation of pivot value
   */
  @SuppressWarnings({ "deprecation", "rawtypes" })
  public String getResult(String expressionName) {
    for (int count = 0; count < expressionNames.length; count++) {
      if (expressionName.equals(expressionNames[count])) {
        Comparable value = expressions[count].getValue();
        if (value.getClass().equals(Date.class)) {
          return TrieDateField.formatExternal((Date)value);
        } else {
          return value.toString();
        }
      }
    }
    throw new SolrException(ErrorCode.BAD_REQUEST, "Pivot expression "+expressionName+" not found.");
  }

  /**
   * Used for JMX stats collecting. Counts the number of stats requests
   * @return number of unique stats collectors
   */
  public long getNumStatsCollectors() {
    return statsCollectors.length;
  }

  /**
   * Used for JMX stats collecting. Counts the number of queries in all query facets
   * @return number of queries requested in all query facets.
   */
  public long getNumQueries() {
    return 0l;
  }
}
