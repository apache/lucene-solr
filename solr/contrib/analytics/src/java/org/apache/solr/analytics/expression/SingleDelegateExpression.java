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

import java.util.Date;

/**
 * Abstraction of an expression that applies a function to one delegate expression.
 */
public abstract class SingleDelegateExpression extends Expression {
  protected Expression delegate;
  
  public SingleDelegateExpression(Expression delegate) {
    this.delegate = delegate;
  }
}
/**
 * <code>NegateExpression</code> returns the negation of the delegate's value.
 */
class NegateExpression extends SingleDelegateExpression {
  public NegateExpression(Expression delegate) {
    super(delegate);
  }

  @Override
  public Comparable getValue() {
    Comparable nComp = delegate.getValue();
    if (nComp==null) {
      return null;
    } else if (nComp.getClass().equals(Date.class)) {
      nComp = new Long(((Date)nComp).getTime());
    }
    return new Double(((Number)nComp).doubleValue()*-1);
  }
}
/**
 * <code>AbsoluteValueExpression</code> returns the negation of the delegate's value.
 */
class AbsoluteValueExpression extends SingleDelegateExpression {
  public AbsoluteValueExpression(Expression delegate) {
    super(delegate);
  }

  @Override
  public Comparable getValue() {
    Comparable nComp = delegate.getValue();
    if (nComp==null) {
      return null;
    }
    double d = ((Number)nComp).doubleValue();
    if (d<0) {
      return new Double(d*-1);
    } else {
      return new Double(d);
    }
  }
}
/**
 * <code>StringExpression</code> returns the reverse of the delegate's string value.
 */
class ReverseExpression extends SingleDelegateExpression {
  public ReverseExpression(Expression delegate) {
    super(delegate);
  }

  @Override
  public Comparable getValue() {
    Comparable rComp = delegate.getValue();
    if (rComp==null) {
      return null;
    }
    return new StringBuilder(rComp.toString()).reverse().toString();
  }
}
