package org.apache.lucene.queryParser.standard.builders;

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

import org.apache.lucene.document.NumericField;
import org.apache.lucene.messages.MessageImpl;
import org.apache.lucene.queryParser.core.QueryNodeException;
import org.apache.lucene.queryParser.core.messages.QueryParserMessages;
import org.apache.lucene.queryParser.core.nodes.QueryNode;
import org.apache.lucene.queryParser.core.util.StringUtils;
import org.apache.lucene.queryParser.standard.config.NumericConfig;
import org.apache.lucene.queryParser.standard.nodes.NumericQueryNode;
import org.apache.lucene.queryParser.standard.nodes.NumericRangeQueryNode;
import org.apache.lucene.search.NumericRangeQuery;

public class NumericRangeQueryNodeBuilder implements StandardQueryBuilder {
  
  public NumericRangeQueryNodeBuilder() {
  // empty constructor
  }
  
  public NumericRangeQuery<? extends Number> build(QueryNode queryNode)
      throws QueryNodeException {
    NumericRangeQueryNode numericRangeNode = (NumericRangeQueryNode) queryNode;
    
    NumericQueryNode lowerNumericNode = numericRangeNode.getLowerBound();
    NumericQueryNode upperNumericNode = numericRangeNode.getUpperBound();
    
    Number lowerNumber, upperNumber;
    
    if (lowerNumericNode != null) {
      lowerNumber = lowerNumericNode.getValue();
    } else {
      lowerNumber = null;
    }
    
    if (upperNumericNode != null) {
      upperNumber = upperNumericNode.getValue();
    } else {
      upperNumber = null;
    }
    
    NumericConfig numericConfig = numericRangeNode.getNumericConfig();
    NumericField.DataType numberType = numericConfig.getType();
    String field = StringUtils.toString(numericRangeNode.getField());
    boolean minInclusive = numericRangeNode.isLowerInclusive();
    boolean maxInclusive = numericRangeNode.isUpperInclusive();
    int precisionStep = numericConfig.getPrecisionStep();
    
    switch (numberType) {
      
      case LONG:
        return NumericRangeQuery.newLongRange(field, precisionStep,
            (Long) lowerNumber, (Long) upperNumber, minInclusive, maxInclusive);
      
      case INT:
        return NumericRangeQuery.newIntRange(field, precisionStep,
            (Integer) lowerNumber, (Integer) upperNumber, minInclusive,
            maxInclusive);
      
      case FLOAT:
        return NumericRangeQuery.newFloatRange(field, precisionStep,
            (Float) lowerNumber, (Float) upperNumber, minInclusive,
            maxInclusive);
      
      case DOUBLE:
        return NumericRangeQuery.newDoubleRange(field, precisionStep,
            (Double) lowerNumber, (Double) upperNumber, minInclusive,
            maxInclusive);
        
        default :
          throw new QueryNodeException(new MessageImpl(
            QueryParserMessages.UNSUPPORTED_NUMERIC_DATA_TYPE, numberType));
        
    }
  }
  
}
