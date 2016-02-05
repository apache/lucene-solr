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
package org.apache.solr.analytics.expression;

/**
 * Abstraction of an expression that applies a function to two delegate expressions.
 */
public abstract class DualDelegateExpression extends Expression {
  protected Expression a;
  protected Expression b;
  public DualDelegateExpression(Expression a, Expression b) {
    this.a = a;
    this.b = b;
  }
}
/**
 * <code>DivideExpression</code> returns the quotient of 'a' and 'b'.
 */
class DivideExpression extends DualDelegateExpression {
  
  /**
   * @param a numerator
   * @param b divisor
   */
  public DivideExpression(Expression a, Expression b) {
    super(a,b);
  }

  @Override
  public Comparable getValue() {
    Comparable aComp = a.getValue();
    Comparable bComp = b.getValue();
    if (aComp==null || bComp==null) {
      return null;
    }
    double div = ((Number)aComp).doubleValue();
    div = div / ((Number)bComp).doubleValue();
    return new Double(div);
  }
}
/**
 * <code>PowerExpression</code> returns 'a' to the power of 'b'.
 */
class PowerExpression extends DualDelegateExpression {

  /**
   * @param a base
   * @param b exponent
   */
  public PowerExpression(Expression a, Expression b) {
    super(a,b);
  }

  @Override
  public Comparable getValue() {
    Comparable aComp = a.getValue();
    Comparable bComp = b.getValue();
    if (aComp==null || bComp==null) {
      return null;
    }
    return new Double(Math.pow(((Number)aComp).doubleValue(),((Number)bComp).doubleValue()));
  }
}
/**
 * <code>LogExpression</code> returns the log of the delegate's value given a base number.
 */
class LogExpression extends DualDelegateExpression {
  /**
   * @param a number
   * @param b base
   */
  public LogExpression(Expression a, Expression b) {
    super(a,b);
  }

  @Override
  public Comparable getValue() {
    Comparable aComp = a.getValue();
    Comparable bComp = b.getValue();
    if (aComp==null || bComp==null) {
      return null;
    }
    return Math.log(((Number)aComp).doubleValue())/Math.log(((Number)bComp).doubleValue());
  }
}
