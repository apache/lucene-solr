package org.apache.lucene.queryParser.standard.nodes;

import org.apache.lucene.document.NumericField;
import org.apache.lucene.messages.MessageImpl;
import org.apache.lucene.queryParser.core.QueryNodeException;
import org.apache.lucene.queryParser.core.messages.QueryParserMessages;
import org.apache.lucene.queryParser.standard.config.NumericConfig;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

public class NumericRangeQueryNode extends
    AbstractRangeQueryNode<NumericQueryNode> {
  
  public NumericConfig numericConfig; 
  
  public NumericRangeQueryNode(NumericQueryNode lower, NumericQueryNode upper,
      boolean lowerInclusive, boolean upperInclusive, NumericConfig numericConfig) throws QueryNodeException {
    setBounds(lower, upper, lowerInclusive, upperInclusive, numericConfig);
  }
  
  private static NumericField.DataType getNumericDataType(Number number) throws QueryNodeException {
    
    if (number instanceof Long) {
      return NumericField.DataType.LONG;
    } else if (number instanceof Integer) {
      return NumericField.DataType.INT;
    } else if (number instanceof Double) {
      return NumericField.DataType.DOUBLE;
    } else if (number instanceof Float) {
      return NumericField.DataType.FLOAT;
    } else {
      throw new QueryNodeException(
          new MessageImpl(
              QueryParserMessages.NUMBER_CLASS_NOT_SUPPORTED_BY_NUMERIC_RANGE_QUERY,
              number.getClass()));
    }
    
  }
  
  public void setBounds(NumericQueryNode lower, NumericQueryNode upper,
      boolean lowerInclusive, boolean upperInclusive, NumericConfig numericConfig) throws QueryNodeException {
    
    if (numericConfig == null) {
      throw new IllegalArgumentException("numericConfig cannot be null!");
    }
    
    NumericField.DataType lowerNumberType, upperNumberType;
    
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
  
  public NumericConfig getNumericConfig() {
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
