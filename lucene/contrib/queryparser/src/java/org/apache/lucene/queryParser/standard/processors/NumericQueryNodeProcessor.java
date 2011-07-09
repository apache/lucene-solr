package org.apache.lucene.queryParser.standard.processors;

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

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.List;

import org.apache.lucene.messages.MessageImpl;
import org.apache.lucene.queryParser.core.QueryNodeException;
import org.apache.lucene.queryParser.core.QueryNodeParseException;
import org.apache.lucene.queryParser.core.config.FieldConfig;
import org.apache.lucene.queryParser.core.config.QueryConfigHandler;
import org.apache.lucene.queryParser.core.messages.QueryParserMessages;
import org.apache.lucene.queryParser.core.nodes.FieldQueryNode;
import org.apache.lucene.queryParser.core.nodes.ParametricQueryNode;
import org.apache.lucene.queryParser.core.nodes.QueryNode;
import org.apache.lucene.queryParser.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryParser.standard.config.NumericConfig;
import org.apache.lucene.queryParser.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryParser.standard.nodes.NumericQueryNode;
import org.apache.lucene.queryParser.standard.nodes.NumericRangeQueryNode;

public class NumericQueryNodeProcessor extends QueryNodeProcessorImpl {
  
  public NumericQueryNodeProcessor() {
  // empty constructor
  }
  
  @Override
  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {
    
    if (node instanceof FieldQueryNode
        && !(node instanceof ParametricQueryNode)) {
      
      QueryConfigHandler config = getQueryConfigHandler();
      
      if (config != null) {
        FieldQueryNode fieldNode = (FieldQueryNode) node;
        FieldConfig fieldConfig = config.getFieldConfig(fieldNode
            .getFieldAsString());
        
        if (fieldConfig != null) {
          NumericConfig numericConfig = fieldConfig
              .get(ConfigurationKeys.NUMERIC_CONFIG);
          
          if (numericConfig != null) {
            
            NumberFormat numberFormat = numericConfig.getNumberFormat();
            Number number;
            
            try {
              number = numberFormat.parse(fieldNode.getTextAsString());
              
            } catch (ParseException e) {
              throw new QueryNodeParseException(new MessageImpl(
                  QueryParserMessages.COULD_NOT_PARSE_NUMBER, fieldNode
                      .getTextAsString(), numberFormat.getClass()
                      .getCanonicalName()), e);
            }
            
            switch (numericConfig.getType()) {
              case LONG:
                number = number.longValue();
                break;
              case INT:
                number = number.intValue();
                break;
              case DOUBLE:
                number = number.doubleValue();
                break;
              case FLOAT:
                number = number.floatValue();
            }
            
            NumericQueryNode lowerNode = new NumericQueryNode(fieldNode
                .getField(), number, numberFormat);
            NumericQueryNode upperNode = new NumericQueryNode(fieldNode
                .getField(), number, numberFormat);
            
            return new NumericRangeQueryNode(lowerNode, upperNode, true, true,
                numericConfig);
            
          }
          
        }
        
      }
      
    }
    
    return node;
    
  }
  
  @Override
  protected QueryNode preProcessNode(QueryNode node) throws QueryNodeException {
    return node;
  }
  
  @Override
  protected List<QueryNode> setChildrenOrder(List<QueryNode> children)
      throws QueryNodeException {
    return children;
  }
  
}
