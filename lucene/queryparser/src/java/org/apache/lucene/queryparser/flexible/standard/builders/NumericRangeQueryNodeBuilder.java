package org.apache.lucene.queryparser.flexible.standard.builders;

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

import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.util.StringUtils;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.config.NumericConfig;
import org.apache.lucene.queryparser.flexible.standard.nodes.NumericQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.NumericRangeQueryNode;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;

/**
 * Builds {@link NumericRangeQuery}s out of {@link NumericRangeQueryNode}s.
 *
 * @see NumericRangeQuery
 * @see NumericRangeQueryNode
 */
public class NumericRangeQueryNodeBuilder implements StandardQueryBuilder {
  
  /**
   * Constructs a {@link NumericRangeQueryNodeBuilder} object.
   */
  public NumericRangeQueryNodeBuilder() {
  // empty constructor
  }
  
  @Override
  public Query build(QueryNode queryNode)
      throws QueryNodeException {
    NumericRangeQueryNode numericRangeNode = (NumericRangeQueryNode) queryNode;
    
    NumericQueryNode lowerNumericNode = numericRangeNode.getLowerBound();
    NumericQueryNode upperNumericNode = numericRangeNode.getUpperBound();
    
    Number lowerNumber = lowerNumericNode.getValue();
    Number upperNumber = upperNumericNode.getValue();
    
    NumericConfig numericConfig = numericRangeNode.getNumericConfig();
    FieldTypes fieldTypes = numericConfig.getFieldTypes();
    String field = StringUtils.toString(numericRangeNode.getField());
    boolean minInclusive = numericRangeNode.isLowerInclusive();
    boolean maxInclusive = numericRangeNode.isUpperInclusive();

    // TODO: we should here check that the incoming Number is correct type:
    Filter filter;
    switch (fieldTypes.getValueType(field)) {
    case INT:
      filter = fieldTypes.newIntRangeFilter(field,
                                            lowerNumber == null ? null : Integer.valueOf(lowerNumber.intValue()),
                                            minInclusive,
                                            upperNumber == null ? null : Integer.valueOf(upperNumber.intValue()),
                                            maxInclusive);
      break;
    case LONG:
      filter = fieldTypes.newLongRangeFilter(field,
                                             lowerNumber == null ? null : Long.valueOf(lowerNumber.longValue()),
                                             minInclusive,
                                             upperNumber == null ? null : Long.valueOf(upperNumber.longValue()),
                                             maxInclusive);
      break;
    case FLOAT:
      filter = fieldTypes.newFloatRangeFilter(field,
                                              lowerNumber == null ? null : Float.valueOf(lowerNumber.floatValue()),
                                              minInclusive,
                                              upperNumber == null ? null : Float.valueOf(upperNumber.floatValue()),
                                              maxInclusive);
      break;
    case DOUBLE:
      filter = fieldTypes.newDoubleRangeFilter(field,
                                               lowerNumber == null ? null : Double.valueOf(lowerNumber.doubleValue()),
                                               minInclusive,
                                               upperNumber == null ? null : Double.valueOf(upperNumber.doubleValue()),
                                               maxInclusive);
      break;
    default:
      throw new IllegalArgumentException("field \"" + field + "\": cannot create numeric query: unhandled valueType " + fieldTypes.getValueType(field));
    }

    return new ConstantScoreQuery(filter);
  }
}
