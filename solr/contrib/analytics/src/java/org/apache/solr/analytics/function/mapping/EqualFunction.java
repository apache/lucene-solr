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
package org.apache.solr.analytics.function.mapping;

import org.apache.solr.analytics.ExpressionFactory.CreatorFunction;
import org.apache.solr.analytics.util.function.BooleanConsumer;
import org.apache.solr.analytics.value.AnalyticsValue;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.BooleanValue;
import org.apache.solr.analytics.value.BooleanValueStream;
import org.apache.solr.analytics.value.BooleanValue.AbstractBooleanValue;
import org.apache.solr.analytics.value.BooleanValueStream.AbstractBooleanValueStream;
import org.apache.solr.analytics.value.DoubleValueStream;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * An equals mapping function, returning whether two values are equal.
 * <p>
 * Uses:
 * <ul>
 * <li>If two Values are passed in, a {@link BooleanValue} representing the equality of the two values for each document is returned.
 * <li>If an {@link AnalyticsValue} and an {@link AnalyticsValueStream} are passed in,
 * a {@link BooleanValueStream} representing the equality of the Value and each of the values of the ValueStream for the document is returned.
 * </ul>
 */
public class EqualFunction {
  public static final String name = "equal";
  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length != 2) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires 2 paramaters, " + params.length + " found.");
    }

    AnalyticsValueStream paramA = params[0];
    AnalyticsValueStream paramB = params[1];

    // Booleans aren't really comparable, so just enable the equal function
    if (paramA instanceof BooleanValueStream && paramB instanceof BooleanValueStream) {
      if (paramA instanceof BooleanValue) {
        if (paramB instanceof BooleanValue) {
          return new BooleanValueEqualFunction((BooleanValue)paramA,(BooleanValue)paramB);
        }
        return new BooleanStreamEqualFunction((BooleanValue)paramA,(BooleanValueStream)paramB);
      } else if (paramB instanceof BooleanValue) {
        return new BooleanStreamEqualFunction((BooleanValue)paramB,(BooleanValueStream)paramA);
      }
    } else if (paramA instanceof DoubleValueStream && paramB instanceof DoubleValueStream) {
      return ComparisonFunction.createComparisonFunction(name, val -> {
        return val == 0;
      }, params);
    } else if (paramA instanceof AnalyticsValue) {
      // This means that the Objects created by the AnalyticsValueStreams are not comparable, so use the .equals() method instead
      if (paramB instanceof AnalyticsValue) {
        return new ValueEqualFunction((AnalyticsValue)paramA,(AnalyticsValue)paramB);
      }
      return new StreamEqualFunction((AnalyticsValue)paramA,paramB);
    } else if (paramB instanceof AnalyticsValue) {
      return new StreamEqualFunction((AnalyticsValue)paramB,paramA);
    }
    throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires that at least 1 parameter be single-valued.");
  });
}
/**
 * An equal function for two {@link BooleanValue}s.
 */
class BooleanValueEqualFunction extends AbstractBooleanValue {
  private final BooleanValue exprA;
  private final BooleanValue exprB;
  public static final String name = EqualFunction.name;
  private final String funcStr;
  private final ExpressionType funcType;

  public BooleanValueEqualFunction(BooleanValue exprA, BooleanValue exprB) {
    this.exprA = exprA;
    this.exprB = exprB;
    this.funcStr = AnalyticsValueStream.createExpressionString(name,exprA,exprB);
    this.funcType = AnalyticsValueStream.determineMappingPhase(funcStr,exprA,exprB);
  }

  private boolean exists = false;
  @Override
  public boolean getBoolean() {
    boolean valueA = exprA.getBoolean();
    boolean valueB = exprB.getBoolean();
    exists = exprA.exists() && exprB.exists();
    return exists ? valueA == valueB : false;
  }
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return funcStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
/**
 * An equal function for a {@link BooleanValue} and a {@link BooleanValueStream}.
 */
class BooleanStreamEqualFunction extends AbstractBooleanValueStream {
  private final BooleanValue baseExpr;
  private final BooleanValueStream compExpr;
  public static final String name = EqualFunction.name;
  private final String funcStr;
  private final ExpressionType funcType;

  public BooleanStreamEqualFunction(BooleanValue baseExpr, BooleanValueStream compExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.compExpr = compExpr;
    this.funcStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(funcStr,baseExpr,compExpr);
  }

  @Override
  public void streamBooleans(BooleanConsumer cons) {
    boolean baseValue = baseExpr.getBoolean();
    if (baseExpr.exists()) {
      compExpr.streamBooleans(compValue -> cons.accept(baseValue == compValue));
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return funcStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
/**
 * A catch-all equal function for two {@link AnalyticsValue}s.
 */
class ValueEqualFunction extends AbstractBooleanValue {
  private final AnalyticsValue exprA;
  private final AnalyticsValue exprB;
  public static final String name = EqualFunction.name;
  private final String funcStr;
  private final ExpressionType funcType;

  public ValueEqualFunction(AnalyticsValue exprA, AnalyticsValue exprB) {
    this.exprA = exprA;
    this.exprB = exprB;
    this.funcStr = AnalyticsValueStream.createExpressionString(name,exprA,exprB);
    this.funcType = AnalyticsValueStream.determineMappingPhase(funcStr,exprA,exprB);
  }

  private boolean exists = false;
  @Override
  public boolean getBoolean() {
    Object valueA = exprA.getObject();
    Object valueB = exprB.getObject();
    exists = exprA.exists() && exprB.exists();
    return exists ? valueA.equals(valueB) : false;
  }
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return funcStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
/**
 * A catch-all equal function for an {@link AnalyticsValue} and an {@link AnalyticsValueStream}.
 */
class StreamEqualFunction extends AbstractBooleanValueStream {
  private final AnalyticsValue baseExpr;
  private final AnalyticsValueStream compExpr;
  public static final String name = EqualFunction.name;
  private final String funcStr;
  private final ExpressionType funcType;

  public StreamEqualFunction(AnalyticsValue baseExpr, AnalyticsValueStream compExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.compExpr = compExpr;
    this.funcStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(funcStr,baseExpr,compExpr);
  }

  @Override
  public void streamBooleans(BooleanConsumer cons) {
    Object baseValue = baseExpr.getObject();
    if (baseExpr.exists()) {
      compExpr.streamObjects(compValue -> cons.accept(baseValue.equals(compValue)));
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return funcStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
