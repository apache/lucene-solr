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
package org.apache.lucene.queryparser.flexible.standard.processors;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.List;

import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.config.FieldConfig;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.core.util.StringUtils;
import org.apache.lucene.queryparser.flexible.standard.config.NumericConfig;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryparser.flexible.standard.nodes.NumericQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.NumericRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.TermRangeQueryNode;

/**
 * This processor is used to convert {@link TermRangeQueryNode}s to
 * {@link NumericRangeQueryNode}s. It looks for
 * {@link ConfigurationKeys#NUMERIC_CONFIG} set in the {@link FieldConfig} of
 * every {@link TermRangeQueryNode} found. If
 * {@link ConfigurationKeys#NUMERIC_CONFIG} is found, it considers that
 * {@link TermRangeQueryNode} to be a numeric range query and convert it to
 * {@link NumericRangeQueryNode}.
 * 
 * @see ConfigurationKeys#NUMERIC_CONFIG
 * @see TermRangeQueryNode
 * @see NumericConfig
 * @see NumericRangeQueryNode
 */
public class NumericRangeQueryNodeProcessor extends QueryNodeProcessorImpl {
  
  /**
   * Constructs an empty {@link NumericRangeQueryNode} object.
   */
  public NumericRangeQueryNodeProcessor() {
  // empty constructor
  }
  
  @Override
  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {
    
    if (node instanceof TermRangeQueryNode) {
      QueryConfigHandler config = getQueryConfigHandler();
      
      if (config != null) {
        TermRangeQueryNode termRangeNode = (TermRangeQueryNode) node;
        FieldConfig fieldConfig = config.getFieldConfig(StringUtils
            .toString(termRangeNode.getField()));
        
        if (fieldConfig != null) {
          
          NumericConfig numericConfig = fieldConfig
              .get(ConfigurationKeys.NUMERIC_CONFIG);
          
          if (numericConfig != null) {
            
            FieldQueryNode lower = termRangeNode.getLowerBound();
            FieldQueryNode upper = termRangeNode.getUpperBound();
            
            String lowerText = lower.getTextAsString();
            String upperText = upper.getTextAsString();
            NumberFormat numberFormat = numericConfig.getNumberFormat();
            Number lowerNumber = null, upperNumber = null;
            
             if (lowerText.length() > 0) {
              
              try {
                lowerNumber = numberFormat.parse(lowerText);
                
              } catch (ParseException e) {
                throw new QueryNodeParseException(new MessageImpl(
                    QueryParserMessages.COULD_NOT_PARSE_NUMBER, lower
                        .getTextAsString(), numberFormat.getClass()
                        .getCanonicalName()), e);
              }
              
            }
            
             if (upperText.length() > 0) {
            
              try {
                upperNumber = numberFormat.parse(upperText);
                
              } catch (ParseException e) {
                throw new QueryNodeParseException(new MessageImpl(
                    QueryParserMessages.COULD_NOT_PARSE_NUMBER, upper
                        .getTextAsString(), numberFormat.getClass()
                        .getCanonicalName()), e);
              }
            
            }
            
            switch (numericConfig.getType()) {
              case LONG:
                if (upperNumber != null) upperNumber = upperNumber.longValue();
                if (lowerNumber != null) lowerNumber = lowerNumber.longValue();
                break;
              case INT:
                if (upperNumber != null) upperNumber = upperNumber.intValue();
                if (lowerNumber != null) lowerNumber = lowerNumber.intValue();
                break;
              case DOUBLE:
                if (upperNumber != null) upperNumber = upperNumber.doubleValue();
                if (lowerNumber != null) lowerNumber = lowerNumber.doubleValue();
                break;
              case FLOAT:
                if (upperNumber != null) upperNumber = upperNumber.floatValue();
                if (lowerNumber != null) lowerNumber = lowerNumber.floatValue();
            }
            
            NumericQueryNode lowerNode = new NumericQueryNode(
                termRangeNode.getField(), lowerNumber, numberFormat);
            NumericQueryNode upperNode = new NumericQueryNode(
                termRangeNode.getField(), upperNumber, numberFormat);
            
            boolean lowerInclusive = termRangeNode.isLowerInclusive();
            boolean upperInclusive = termRangeNode.isUpperInclusive();
            
            return new NumericRangeQueryNode(lowerNode, upperNode,
                lowerInclusive, upperInclusive, numericConfig);
            
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
