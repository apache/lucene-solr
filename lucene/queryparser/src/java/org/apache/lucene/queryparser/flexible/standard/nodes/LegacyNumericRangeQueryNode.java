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

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FieldType.LegacyNumericType;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.config.LegacyNumericConfig;

/**
 * This query node represents a range query composed by {@link LegacyNumericQueryNode}
 * bounds, which means the bound values are {@link Number}s.
 * 
 * @see LegacyNumericQueryNode
 * @see AbstractRangeQueryNode
 * @deprecated Index with Points instead and use {@link PointRangeQueryNode} instead.
 */
@Deprecated
public class LegacyNumericRangeQueryNode extends
    AbstractRangeQueryNode<LegacyNumericQueryNode> {
  
  public LegacyNumericConfig numericConfig; 
  
  /**
   * Constructs a {@link LegacyNumericRangeQueryNode} object using the given
   * {@link LegacyNumericQueryNode} as its bounds and {@link LegacyNumericConfig}.
   * 
   * @param lower the lower bound
   * @param upper the upper bound
   * @param lowerInclusive <code>true</code> if the lower bound is inclusive, otherwise, <code>false</code>
   * @param upperInclusive <code>true</code> if the upper bound is inclusive, otherwise, <code>false</code>
   * @param numericConfig the {@link LegacyNumericConfig} that represents associated with the upper and lower bounds
   * 
   * @see #setBounds(LegacyNumericQueryNode, LegacyNumericQueryNode, boolean, boolean, LegacyNumericConfig)
   */
  public LegacyNumericRangeQueryNode(LegacyNumericQueryNode lower, LegacyNumericQueryNode upper,
      boolean lowerInclusive, boolean upperInclusive, LegacyNumericConfig numericConfig) throws QueryNodeException {
    setBounds(lower, upper, lowerInclusive, upperInclusive, numericConfig);
  }
  
  private static LegacyNumericType getNumericDataType(Number number) throws QueryNodeException {
    
    if (number instanceof Long) {
      return FieldType.LegacyNumericType.LONG;
    } else if (number instanceof Integer) {
      return FieldType.LegacyNumericType.INT;
    } else if (number instanceof Double) {
      return LegacyNumericType.DOUBLE;
    } else if (number instanceof Float) {
      return FieldType.LegacyNumericType.FLOAT;
    } else {
      throw new QueryNodeException(
          new MessageImpl(
              QueryParserMessages.NUMBER_CLASS_NOT_SUPPORTED_BY_NUMERIC_RANGE_QUERY,
              number.getClass()));
    }
    
  }
  
  /**
   * Sets the upper and lower bounds of this range query node and the
   * {@link LegacyNumericConfig} associated with these bounds.
   * 
   * @param lower the lower bound
   * @param upper the upper bound
   * @param lowerInclusive <code>true</code> if the lower bound is inclusive, otherwise, <code>false</code>
   * @param upperInclusive <code>true</code> if the upper bound is inclusive, otherwise, <code>false</code>
   * @param numericConfig the {@link LegacyNumericConfig} that represents associated with the upper and lower bounds
   * 
   */
  public void setBounds(LegacyNumericQueryNode lower, LegacyNumericQueryNode upper,
      boolean lowerInclusive, boolean upperInclusive, LegacyNumericConfig numericConfig) throws QueryNodeException {
    
    if (numericConfig == null) {
      throw new IllegalArgumentException("numericConfig must not be null!");
    }
    
    LegacyNumericType lowerNumberType, upperNumberType;
    
    if (lower != null && lower.getValue() != null) {
      lowerNumberType = getNumericDataType(lower.getValue());
    } else {
      lowerNumberType = null;
    }
    
    if (upper != null && upper.getValue() != null) {
      upperNumberType = getNumericDataType(upper.getValue());
    } else {
      upperNumberType = null;
    }
    
    if (lowerNumberType != null
        && !lowerNumberType.equals(numericConfig.getType())) {
      throw new IllegalArgumentException(
          "lower value's type should be the same as numericConfig type: "
              + lowerNumberType + " != " + numericConfig.getType());
    }
    
    if (upperNumberType != null
        && !upperNumberType.equals(numericConfig.getType())) {
      throw new IllegalArgumentException(
          "upper value's type should be the same as numericConfig type: "
              + upperNumberType + " != " + numericConfig.getType());
    }
    
    super.setBounds(lower, upper, lowerInclusive, upperInclusive);
    this.numericConfig = numericConfig;
    
  }
  
  /**
   * Returns the {@link LegacyNumericConfig} associated with the lower and upper bounds.
   * 
   * @return the {@link LegacyNumericConfig} associated with the lower and upper bounds
   */
  public LegacyNumericConfig getNumericConfig() {
    return this.numericConfig;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("<numericRange lowerInclusive='");
    
    sb.append(isLowerInclusive()).append("' upperInclusive='").append(
        isUpperInclusive()).append(
        "' precisionStep='" + numericConfig.getPrecisionStep()).append(
        "' type='" + numericConfig.getType()).append("'>\n");
    
    sb.append(getLowerBound()).append('\n');
    sb.append(getUpperBound()).append('\n');
    sb.append("</numericRange>");
    
    return sb.toString();
    
  }
  
}
