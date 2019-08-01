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
package org.apache.solr.analytics.function;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.analytics.AnalyticsExpression;
import org.apache.solr.analytics.function.ReductionCollectionManager.ReductionDataCollection;
import org.apache.solr.analytics.util.AnalyticsResponseHeadings;
import org.apache.solr.common.util.NamedList;

/**
 * A class used to generate results for a list of {@link AnalyticsExpression}s.
 */
public class ExpressionCalculator {
  private final Iterable<AnalyticsExpression> expressions;

  public ExpressionCalculator(Iterable<AnalyticsExpression> expressions) {
    this.expressions = expressions;
  }

  /**
   * Calculate results for the list of {@link AnalyticsExpression}s.
   * <p>
   * NOTE: This method can, and is, called multiple times to generate different responses.
   * <br>
   * The results are determined by which {@link ReductionDataCollection} is passed to the {@link ReductionCollectionManager#setData}
   * method of the {@link ReductionCollectionManager} managing the reduction for the list of {@link AnalyticsExpression}s.
   *
   * @return a {@link NamedList} containing the results
   */
  public Map<String,Object> getResults() {
    Map<String,Object> exprVals = new HashMap<>();
    expressions.forEach(expr -> {
      Object obj = expr.toObject();
      if (expr.exists()) {
        exprVals.put(expr.getName(), obj);
      }
    });
    return exprVals;
  }

  /**
   * Calculate results for the list of {@link AnalyticsExpression}s and add them to the given response.
   * <p>
   * NOTE: This method can, and is, called multiple times to generate different responses.
   * <br>
   * The results are determined by which {@link ReductionDataCollection} is passed to the {@link ReductionCollectionManager#setData}
   * method of the {@link ReductionCollectionManager} managing the reduction for the list of {@link AnalyticsExpression}s.
   *
   * @param response the response to add the results map to.
   */
  public void addResults(Map<String,Object> response) {
    response.put(AnalyticsResponseHeadings.RESULTS, getResults());
  }
}
