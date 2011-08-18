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

import java.text.Collator;

import org.apache.lucene.queryParser.core.nodes.FieldQueryNode;
import org.apache.lucene.queryParser.core.nodes.ParametricQueryNode;
import org.apache.lucene.queryParser.core.nodes.ParametricQueryNode.CompareOperator;
import org.apache.lucene.queryParser.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryParser.standard.processors.ParametricRangeQueryNodeProcessor;

/**
 * This query node represents a range query. It also holds which collator will
 * be used by the range query and if the constant score rewrite is enabled. <br/>
 * 
 * @see ParametricRangeQueryNodeProcessor
 * @see ConfigurationKeys#RANGE_COLLATOR
 * @see org.apache.lucene.search.TermRangeQuery
 * 
 * @deprecated this class will be removed in future, {@link TermRangeQueryNode} should
 * be used instead
 */
@Deprecated
public class RangeQueryNode extends TermRangeQueryNode {

  private static final long serialVersionUID = 7400866652044314657L;

  private Collator collator;

  /**
   * @param lower
   * @param upper
   */
  public RangeQueryNode(ParametricQueryNode lower, ParametricQueryNode upper,
      Collator collator) {
    
    super(lower, upper, lower.getOperator() == CompareOperator.LE, upper
        .getOperator() == CompareOperator.GE);
    
    this.collator = collator;
    
  }
  
  @Override
  public ParametricQueryNode getLowerBound() {
    return (ParametricQueryNode) super.getLowerBound();
  }
  
  @Override
  public ParametricQueryNode getUpperBound() {
    return (ParametricQueryNode) super.getUpperBound();
  }
  
  /**
   * Sets lower and upper bounds. The method signature expects
   * {@link FieldQueryNode} objects as lower and upper, however,
   * an {@link IllegalArgumentException} will be thrown at runtime
   * if a non {@link ParametricQueryNode} is passed as lower and upper.
   * 
   * @param lower a {@link ParametricQueryNode} object
   * @param upper a {@link ParametricQueryNode} object
   * @param lowerInclusive <code>true</code> if lower bound is inclusive, otherwise, <code>false</code>
   * @param upperInclusive <code>true</code> if upper bound is inclusive, otherwise, <code>false</code>
   * 
   * @throws IllegalArgumentException if lower or upper are not instance of {@link ParametricQueryNode}
   * 
   * @see AbstractRangeQueryNode#setBounds
   */
  @Override
  public void setBounds(FieldQueryNode lower, FieldQueryNode upper,
      boolean lowerInclusive, boolean upperInclusive) {
    
    if (lower != null && !(lower instanceof ParametricQueryNode)) {
      throw new IllegalArgumentException("lower should be an instance of "
          + ParametricQueryNode.class.getCanonicalName() + ", but found "
          + lower.getClass().getCanonicalName());
    }
    
    if (upper != null && !(upper instanceof ParametricQueryNode)) {
      throw new IllegalArgumentException("upper should be an instance of "
          + ParametricQueryNode.class.getCanonicalName() + ", but found "
          + lower.getClass().getCanonicalName());
    }
    
    super.setBounds(lower, upper, lowerInclusive, upperInclusive);
    
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("<range>\n\t");
    sb.append(this.getUpperBound()).append("\n\t");
    sb.append(this.getLowerBound()).append("\n");
    sb.append("</range>\n");

    return sb.toString();

  }

  /**
   * @return the collator
   */
  public Collator getCollator() {
    return this.collator;
  }
  
}
