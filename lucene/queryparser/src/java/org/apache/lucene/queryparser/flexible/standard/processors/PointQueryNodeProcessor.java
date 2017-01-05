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
import org.apache.lucene.queryparser.flexible.core.nodes.RangeQueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryparser.flexible.standard.nodes.PointQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.PointRangeQueryNode;

/**
 * This processor is used to convert {@link FieldQueryNode}s to
 * {@link PointRangeQueryNode}s. It looks for
 * {@link ConfigurationKeys#POINTS_CONFIG} set in the {@link FieldConfig} of
 * every {@link FieldQueryNode} found. If
 * {@link ConfigurationKeys#POINTS_CONFIG} is found, it considers that
 * {@link FieldQueryNode} to be a numeric query and convert it to
 * {@link PointRangeQueryNode} with upper and lower inclusive and lower and
 * upper equals to the value represented by the {@link FieldQueryNode} converted
 * to {@link Number}. It means that <b>field:1</b> is converted to <b>field:[1
 * TO 1]</b>. <br>
 * <br>
 * Note that {@link FieldQueryNode}s children of a
 * {@link RangeQueryNode} are ignored.
 * 
 * @see ConfigurationKeys#POINTS_CONFIG
 * @see FieldQueryNode
 * @see PointsConfig
 * @see PointQueryNode
 */
public class PointQueryNodeProcessor extends QueryNodeProcessorImpl {
  
  /**
   * Constructs a {@link PointQueryNodeProcessor} object.
   */
  public PointQueryNodeProcessor() {
  // empty constructor
  }
  
  @Override
  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {
    
    if (node instanceof FieldQueryNode
        && !(node.getParent() instanceof RangeQueryNode)) {
      
      QueryConfigHandler config = getQueryConfigHandler();
      
      if (config != null) {
        FieldQueryNode fieldNode = (FieldQueryNode) node;
        FieldConfig fieldConfig = config.getFieldConfig(fieldNode
            .getFieldAsString());
        
        if (fieldConfig != null) {
          PointsConfig numericConfig = fieldConfig.get(ConfigurationKeys.POINTS_CONFIG);
          
          if (numericConfig != null) {
            
            NumberFormat numberFormat = numericConfig.getNumberFormat();
            String text = fieldNode.getTextAsString();
            Number number = null;
            
            if (text.length() > 0) {
              
              try {
                number = numberFormat.parse(text);
                
              } catch (ParseException e) {
                throw new QueryNodeParseException(new MessageImpl(
                    QueryParserMessages.COULD_NOT_PARSE_NUMBER, fieldNode
                        .getTextAsString(), numberFormat.getClass()
                        .getCanonicalName()), e);
              }
              
              if (Integer.class.equals(numericConfig.getType())) {
                number = number.intValue();
              } else if (Long.class.equals(numericConfig.getType())) {
                number = number.longValue();
              } else if (Double.class.equals(numericConfig.getType())) {
                number = number.doubleValue();
              } else if (Float.class.equals(numericConfig.getType())) {
                number = number.floatValue();
              }
              
            } else {
              throw new QueryNodeParseException(new MessageImpl(
                  QueryParserMessages.NUMERIC_CANNOT_BE_EMPTY, fieldNode.getFieldAsString()));
            }
            
            PointQueryNode lowerNode = new PointQueryNode(fieldNode.getField(), number, numberFormat);
            PointQueryNode upperNode = new PointQueryNode(fieldNode.getField(), number, numberFormat);
            
            return new PointRangeQueryNode(lowerNode, upperNode, true, true, numericConfig);
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
  protected List<QueryNode> setChildrenOrder(List<QueryNode> children) throws QueryNodeException {
    return children;
  }
}
