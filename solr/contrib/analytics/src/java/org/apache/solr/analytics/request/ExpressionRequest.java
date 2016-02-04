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
package org.apache.solr.analytics.request;

import org.apache.solr.analytics.expression.Expression;

/**
 * Contains name and string representation of an expression.
 */
public class ExpressionRequest implements Comparable<ExpressionRequest> {
  private String name;
  private String expressionString;
  private Expression expression;
  
  /**
   * @param name The name of the Expression.
   * @param expressionString The string representation of the desired Expression.
   */
  public ExpressionRequest(String name, String expressionString) {
    this.name = name;
    this.expressionString = expressionString;
  }

  public void setExpressionString(String expressionString) {
    this.expressionString = expressionString;
  }
  
  public String getExpressionString() {
    return expressionString;
  }
  
  public void setExpression(Expression expression) {
    this.expression = expression;
  }
  
  public Expression getExpression() {
    return expression;
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public String getName() {
    return name;
  }

  @Override
  public int compareTo(ExpressionRequest o) {
    return name.compareTo(o.getName());
  }
  
  @Override
  public String toString() {
    return "<ExpressionRequest name=" + name + " expression=" + expressionString + "/>";
  }
  
}
