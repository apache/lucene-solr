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

import java.text.ParseException;
import java.util.Date;

import org.apache.solr.util.DateMathParser;

/**
 * Abstraction of an expression that applies a function to an array of delegate expressions.
 */
public abstract class MultiDelegateExpression extends Expression {
  protected final Expression[] delegates;
  
  public MultiDelegateExpression(Expression[] delegates) {
    this.delegates = delegates;
  }
}
/**
 * <code>AddExpression</code> returns the sum of its components' values.
 */
class AddExpression extends MultiDelegateExpression {
  public AddExpression(Expression[] delegates) {
    super(delegates);
  }

  @Override
  public Comparable getValue() {
    double sum = 0;
    for (Expression delegate : delegates) {
      Comparable dComp = delegate.getValue();
      if (dComp==null) {
        return null;
      } else if (dComp.getClass().equals(Date.class)) {
        dComp = new Long(((Date)dComp).getTime());
      }
      sum += ((Number)dComp).doubleValue();
    }
    return new Double(sum);
  }
}
/**
 * <code>MultiplyExpression</code> returns the product of its delegates' values.
 */
class MultiplyExpression extends MultiDelegateExpression {
  public MultiplyExpression(Expression[] delegates) {
    super(delegates);
  }

  @Override
  public Comparable getValue() {
    double prod = 1;
    for (Expression delegate : delegates) {
      Comparable dComp = delegate.getValue();
      if (dComp==null) {
        return null;
      }
      prod *= ((Number)dComp).doubleValue();
    }
    return new Double(prod);
  }
}
/**
 * <code>DateMathExpression</code> returns the start date modified by the DateMath operations
 */
class DateMathExpression extends MultiDelegateExpression {
  /**
   * @param delegates A list of Expressions. The first element in the list
   * should be a numeric Expression which represents the starting date. 
   * The rest of the field should be string Expression objects which contain
   * the DateMath operations to perform on the start date.
   */
  public DateMathExpression(Expression[] delegates) {
    super(delegates);
  }

  @Override
  public Comparable getValue() {
    DateMathParser parser = new DateMathParser();
    parser.setNow((Date)delegates[0].getValue());
    try {
      for (int count = 1; count<delegates.length; count++) {
        Comparable dComp = delegates[count].getValue();
        if (dComp==null) {
          return null;
        }
        parser.setNow(parser.parseMath((String)dComp));
      }
      return parser.getNow();
    } catch (ParseException e) {
      e.printStackTrace();
      return parser.getNow();
    }
  }
}
/**
 * <code>ConcatenateExpression</code> returns the concatenation of its delegates' values in the order given.
 */
class ConcatenateExpression extends MultiDelegateExpression {
  public ConcatenateExpression(Expression[] delegates) {
    super(delegates);
  }

  @Override
  public Comparable getValue() {
    StringBuilder builder = new StringBuilder();
    for (Expression delegate : delegates) {
      Comparable dComp = delegate.getValue();
      if (dComp==null) {
        return null;
      }
      builder.append(dComp.toString());
    }
    return builder.toString();
  }
}
