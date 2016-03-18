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

import org.apache.lucene.document.FieldType;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.util.StringUtils;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.config.LegacyNumericConfig;
import org.apache.lucene.queryparser.flexible.standard.nodes.LegacyNumericQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.LegacyNumericRangeQueryNode;
import org.apache.lucene.search.LegacyNumericRangeQuery;

/**
 * Builds {@link org.apache.lucene.search.LegacyNumericRangeQuery}s out of {@link LegacyNumericRangeQueryNode}s.
 *
 * @see org.apache.lucene.search.LegacyNumericRangeQuery
 * @see LegacyNumericRangeQueryNode
 * @deprecated Index with points and use {@link PointRangeQueryNodeBuilder} instead.
 */
@Deprecated
public class LegacyNumericRangeQueryNodeBuilder implements StandardQueryBuilder {
  
  /**
   * Constructs a {@link LegacyNumericRangeQueryNodeBuilder} object.
   */
  public LegacyNumericRangeQueryNodeBuilder() {
  // empty constructor
  }
  
  @Override
  public LegacyNumericRangeQuery<? extends Number> build(QueryNode queryNode)
      throws QueryNodeException {
    LegacyNumericRangeQueryNode numericRangeNode = (LegacyNumericRangeQueryNode) queryNode;
    
    LegacyNumericQueryNode lowerNumericNode = numericRangeNode.getLowerBound();
    LegacyNumericQueryNode upperNumericNode = numericRangeNode.getUpperBound();
    
    Number lowerNumber = lowerNumericNode.getValue();
    Number upperNumber = upperNumericNode.getValue();
    
    LegacyNumericConfig numericConfig = numericRangeNode.getNumericConfig();
    FieldType.LegacyNumericType numberType = numericConfig.getType();
    String field = StringUtils.toString(numericRangeNode.getField());
    boolean minInclusive = numericRangeNode.isLowerInclusive();
    boolean maxInclusive = numericRangeNode.isUpperInclusive();
    int precisionStep = numericConfig.getPrecisionStep();
    
    switch (numberType) {
      
      case LONG:
        return LegacyNumericRangeQuery.newLongRange(field, precisionStep,
            (Long) lowerNumber, (Long) upperNumber, minInclusive, maxInclusive);
      
      case INT:
        return LegacyNumericRangeQuery.newIntRange(field, precisionStep,
            (Integer) lowerNumber, (Integer) upperNumber, minInclusive,
            maxInclusive);
      
      case FLOAT:
        return LegacyNumericRangeQuery.newFloatRange(field, precisionStep,
            (Float) lowerNumber, (Float) upperNumber, minInclusive,
            maxInclusive);
      
      case DOUBLE:
        return LegacyNumericRangeQuery.newDoubleRange(field, precisionStep,
            (Double) lowerNumber, (Double) upperNumber, minInclusive,
            maxInclusive);
        
        default :
          throw new QueryNodeException(new MessageImpl(
            QueryParserMessages.UNSUPPORTED_NUMERIC_DATA_TYPE, numberType));
        
    }
  }
  
}
