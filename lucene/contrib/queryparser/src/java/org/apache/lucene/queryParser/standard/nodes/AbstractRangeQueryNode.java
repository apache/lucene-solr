package org.apache.lucene.queryParser.standard.nodes;

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

import java.util.ArrayList;

import org.apache.lucene.queryParser.core.nodes.FieldValuePairQueryNode;
import org.apache.lucene.queryParser.core.nodes.FieldableNode;
import org.apache.lucene.queryParser.core.nodes.QueryNode;
import org.apache.lucene.queryParser.core.nodes.QueryNodeImpl;
import org.apache.lucene.queryParser.core.parser.EscapeQuerySyntax;
import org.apache.lucene.queryParser.core.util.StringUtils;

public abstract class AbstractRangeQueryNode<T extends FieldValuePairQueryNode<?>>
    extends QueryNodeImpl implements FieldableNode {
  
  private boolean lowerInclusive, upperInclusive;
  
  protected AbstractRangeQueryNode() {
    setLeaf(false);
    allocate();
  }
  
  public CharSequence getField() {
    CharSequence field = null;
    T lower = getLowerBound();
    T upper = getUpperBound();
    
    if (lower != null) {
      field = lower.getField();
      
    } else if (upper != null) {
      field = upper.getField();
    }
    
    return field;
    
  }
  
  public void setField(CharSequence fieldName) {
    T lower = getLowerBound();
    T upper = getUpperBound();
    
    if (lower != null) {
      lower.setField(fieldName);
    }
    
    if (upper != null) {
      upper.setField(fieldName);
    }
    
  }
  
  @SuppressWarnings("unchecked")
  public T getLowerBound() {
    return (T) getChildren().get(0);
  }
  
  @SuppressWarnings("unchecked")
  public T getUpperBound() {
    return (T) getChildren().get(1);
  }
  
  public boolean isLowerInclusive() {
    return lowerInclusive;
  }
  
  public boolean isUpperInclusive() {
    return upperInclusive;
  }
  
  public void setBounds(T lower, T upper, boolean lowerInclusive,
      boolean upperInclusive) {
    
    if (lower != null && upper != null) {
      String lowerField = StringUtils.toString(lower.getField());
      String upperField = StringUtils.toString(upper.getField());
      
      if ((upperField == null && lowerField == null)
          || (upperField != null && !upperField.equals(lowerField))) {
        throw new IllegalArgumentException(
            "lower and upper bounds should have the same field name!");
      }
      
      this.lowerInclusive = lowerInclusive;
      this.upperInclusive = upperInclusive;
      
      ArrayList<QueryNode> children = new ArrayList<QueryNode>(2);
      children.add(lower);
      children.add(upper);
      
      set(children);
      
    }
    
  }
  
  public CharSequence toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
    StringBuilder sb = new StringBuilder();
    
    T lower = getLowerBound();
    T upper = getUpperBound();
    
    if (lowerInclusive) {
      sb.append('[');
      
    } else {
      sb.append('{');
    }
    
    if (lower != null) {
      sb.append(lower.toQueryString(escapeSyntaxParser));
      
    } else {
      sb.append("...");
    }
    
    sb.append(' ');
    
    if (upper != null) {
      sb.append(upper.toQueryString(escapeSyntaxParser));
      
    } else {
      sb.append("...");
    }
    
    if (upperInclusive) {
      sb.append(']');
      
    } else {
      sb.append('}');
    }
    
    return sb.toString();
    
  }
  
}
