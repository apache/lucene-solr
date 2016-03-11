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
package org.apache.lucene.queryparser.flexible.standard.builders;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.util.StringUtils;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.queryparser.flexible.standard.nodes.PointQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.PointRangeQueryNode;
import org.apache.lucene.search.Query;

/**
 * Builds {@link PointValues} range queries out of {@link PointRangeQueryNode}s.
 *
 * @see PointRangeQueryNode
 */
public class PointRangeQueryNodeBuilder implements StandardQueryBuilder {
  
  /**
   * Constructs a {@link PointRangeQueryNodeBuilder} object.
   */
  public PointRangeQueryNodeBuilder() {
  // empty constructor
  }
  
  @Override
  public Query build(QueryNode queryNode) throws QueryNodeException {
    PointRangeQueryNode numericRangeNode = (PointRangeQueryNode) queryNode;
    
    PointQueryNode lowerNumericNode = numericRangeNode.getLowerBound();
    PointQueryNode upperNumericNode = numericRangeNode.getUpperBound();
    
    Number lowerNumber = lowerNumericNode.getValue();
    Number upperNumber = upperNumericNode.getValue();
    
    PointsConfig pointsConfig = numericRangeNode.getPointsConfig();
    Class<? extends Number> numberType = pointsConfig.getType();
    String field = StringUtils.toString(numericRangeNode.getField());
    boolean minInclusive = numericRangeNode.isLowerInclusive();
    boolean maxInclusive = numericRangeNode.isUpperInclusive();
    
    // TODO: push down cleaning up of crazy nulls and inclusive/exclusive elsewhere
    if (Integer.class.equals(numberType)) {
      Integer lower = (Integer) lowerNumber;
      if (lower == null) {
        lower = Integer.MIN_VALUE;
      }
      if (minInclusive == false) {
        lower = lower + 1;
      }
      
      Integer upper = (Integer) upperNumber;
      if (upper == null) {
        upper = Integer.MAX_VALUE;
      }
      if (maxInclusive == false) {
        upper = upper - 1;
      }
      return IntPoint.newRangeQuery(field, lower, upper);
    } else if (Long.class.equals(numberType)) {
      Long lower = (Long) lowerNumber;
      if (lower == null) {
        lower = Long.MIN_VALUE;
      }
      if (minInclusive == false) {
        lower = lower + 1;
      }
      
      Long upper = (Long) upperNumber;
      if (upper == null) {
        upper = Long.MAX_VALUE;
      }
      if (maxInclusive == false) {
        upper = upper - 1;
      }
      return LongPoint.newRangeQuery(field, lower, upper);
    } else if (Float.class.equals(numberType)) {
      Float lower = (Float) lowerNumber;
      if (lower == null) {
        lower = Float.NEGATIVE_INFINITY;
      }
      if (minInclusive == false) {
        lower = Math.nextUp(lower);
      }
      
      Float upper = (Float) upperNumber;
      if (upper == null) {
        upper = Float.POSITIVE_INFINITY;
      }
      if (maxInclusive == false) {
        upper = Math.nextDown(upper);
      }
      return FloatPoint.newRangeQuery(field, lower, upper);
    } else if (Double.class.equals(numberType)) {
      Double lower = (Double) lowerNumber;
      if (lower == null) {
        lower = Double.NEGATIVE_INFINITY;
      }
      if (minInclusive == false) {
        lower = Math.nextUp(lower);
      }
      
      Double upper = (Double) upperNumber;
      if (upper == null) {
        upper = Double.POSITIVE_INFINITY;
      }
      if (maxInclusive == false) {
        upper = Math.nextDown(upper);
      }
      return DoublePoint.newRangeQuery(field, lower, upper);
    } else {
      throw new QueryNodeException(new MessageImpl(QueryParserMessages.UNSUPPORTED_NUMERIC_DATA_TYPE, numberType));
    }
  }
}
