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
package org.apache.lucene.queryparser.flexible.standard.nodes;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;

/**
 * This query node represents a range query composed by {@link PointQueryNode}
 * bounds, which means the bound values are {@link Number}s.
 * 
 * @see PointQueryNode
 * @see AbstractRangeQueryNode
 */
public class PointRangeQueryNode extends AbstractRangeQueryNode<PointQueryNode> {
  
  public PointsConfig numericConfig; 
  
  /**
   * Constructs a {@link PointRangeQueryNode} object using the given
   * {@link PointQueryNode} as its bounds and {@link PointsConfig}.
   * 
   * @param lower the lower bound
   * @param upper the upper bound
   * @param lowerInclusive <code>true</code> if the lower bound is inclusive, otherwise, <code>false</code>
   * @param upperInclusive <code>true</code> if the upper bound is inclusive, otherwise, <code>false</code>
   * @param numericConfig the {@link PointsConfig} that represents associated with the upper and lower bounds
   * 
   * @see #setBounds(PointQueryNode, PointQueryNode, boolean, boolean, PointsConfig)
   */
  public PointRangeQueryNode(PointQueryNode lower, PointQueryNode upper,
      boolean lowerInclusive, boolean upperInclusive, PointsConfig numericConfig) throws QueryNodeException {
    setBounds(lower, upper, lowerInclusive, upperInclusive, numericConfig);
  }
  
  /**
   * Sets the upper and lower bounds of this range query node and the
   * {@link PointsConfig} associated with these bounds.
   * 
   * @param lower the lower bound
   * @param upper the upper bound
   * @param lowerInclusive <code>true</code> if the lower bound is inclusive, otherwise, <code>false</code>
   * @param upperInclusive <code>true</code> if the upper bound is inclusive, otherwise, <code>false</code>
   * @param pointsConfig the {@link PointsConfig} that represents associated with the upper and lower bounds
   * 
   */
  public void setBounds(PointQueryNode lower, PointQueryNode upper,
      boolean lowerInclusive, boolean upperInclusive, PointsConfig pointsConfig) throws QueryNodeException {
    
    if (pointsConfig == null) {
      throw new IllegalArgumentException("pointsConfig must not be null!");
    }
    
    Class<? extends Number> lowerNumberType, upperNumberType;
    
    if (lower != null && lower.getValue() != null) {
      lowerNumberType = lower.getValue().getClass();
    } else {
      lowerNumberType = null;
    }
    
    if (upper != null && upper.getValue() != null) {
      upperNumberType = upper.getValue().getClass();
    } else {
      upperNumberType = null;
    }
    
    if (lowerNumberType != null
        && !lowerNumberType.equals(pointsConfig.getType())) {
      throw new IllegalArgumentException(
          "lower value's type should be the same as numericConfig type: "
              + lowerNumberType + " != " + pointsConfig.getType());
    }
    
    if (upperNumberType != null
        && !upperNumberType.equals(pointsConfig.getType())) {
      throw new IllegalArgumentException(
          "upper value's type should be the same as numericConfig type: "
              + upperNumberType + " != " + pointsConfig.getType());
    }
    
    super.setBounds(lower, upper, lowerInclusive, upperInclusive);
    this.numericConfig = pointsConfig;
  }
  
  /**
   * Returns the {@link PointsConfig} associated with the lower and upper bounds.
   * 
   * @return the {@link PointsConfig} associated with the lower and upper bounds
   */
  public PointsConfig getPointsConfig() {
    return this.numericConfig;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("<pointRange lowerInclusive='");
    sb.append(isLowerInclusive());
    sb.append("' upperInclusive='");
    sb.append(isUpperInclusive());
    sb.append("' type='");
    sb.append(numericConfig.getType().getSimpleName());
    sb.append("'>\n");
    sb.append(getLowerBound()).append('\n');
    sb.append(getUpperBound()).append('\n');
    sb.append("</pointRange>");
    return sb.toString();
  }
}
