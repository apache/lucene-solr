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
import org.apache.solr.analytics.value.DateValue;
import org.apache.solr.analytics.value.DateValueStream;
import org.apache.solr.analytics.value.DoubleValue;
import org.apache.solr.analytics.value.DoubleValueStream;
import org.apache.solr.analytics.value.StringValue;
import org.apache.solr.analytics.value.StringValueStream;
import org.apache.solr.analytics.value.BooleanValue.AbstractBooleanValue;
import org.apache.solr.analytics.value.BooleanValueStream.AbstractBooleanValueStream;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * Contains all comparable functions. Comparable functions accept two parameters and return a BooleanValueStream.
 * The two parameters must be able to be cast to the same type.
 * <p>
 * Uses:
 * <ul>
 * <li>If a two comparable {@link AnalyticsValue}s are passed in, a {@link BooleanValue} representing the comparison of the two values for each document is returned.
 * <li>If a comparable {@link AnalyticsValue} and a comparable {@link AnalyticsValueStream} are passed in, 
 * a {@link BooleanValueStream} representing the comparison of the Value and each of the values of the ValueStream for the document is returned.
 * </ul>
 */
public class CompareFunction {
  
  private static BooleanValueStream createCompareFunction(String name, CompResultFunction comp, AnalyticsValueStream... params) {
    if (params.length != 2) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires 2 paramaters, " + params.length + " found.");
    }
    AnalyticsValueStream paramA = params[0];
    AnalyticsValueStream paramB = params[1];
    if (paramA instanceof DateValueStream && paramB instanceof DateValueStream) {
      if (paramA instanceof DateValue) {
        if (paramB instanceof DateValue) {
          return new CompareDateValueFunction(name,(DateValue)paramA,(DateValue)paramB,comp);
        }
        return new CompareDateStreamFunction(name,(DateValue)paramA,(DateValueStream)paramB,comp);
      }
      if (paramB instanceof DateValue) {
        return new CompareDateStreamFunction(name,(DateValue)paramB,(DateValueStream)paramA,reverse(comp));
      }
    } else if (paramA instanceof DoubleValueStream && paramB instanceof DoubleValueStream) {
      if (paramA instanceof DoubleValue) {
        if (paramB instanceof DoubleValue) {
          return new CompareDoubleValueFunction(name,(DoubleValue)paramA,(DoubleValue)paramB,comp);
        }
        return new CompareDoubleStreamFunction(name,(DoubleValue)paramA,(DoubleValueStream)paramB,comp);
      }
      if (paramB instanceof DoubleValue) {
        return new CompareDoubleStreamFunction(name,(DoubleValue)paramB,(DoubleValueStream)paramA,reverse(comp));
      }
    } else if (paramA instanceof StringValueStream && paramB instanceof StringValueStream) {
      if (paramA instanceof StringValue) {
        if (paramB instanceof StringValue) {
          return new CompareStringValueFunction(name,(StringValue)paramA,(StringValue)paramB,comp);
        }
        return new CompareStringStreamFunction(name,(StringValue)paramA,(StringValueStream)paramB,comp);
      }
      if (paramB instanceof StringValue) {
        return new CompareStringStreamFunction(name,(StringValue)paramB,(StringValueStream)paramA,reverse(comp));
      }
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires comparable parameters.");
    }
    throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires that at least 1 parameter be single-valued.");
  }
  
  /**
   * A comparison function that tests equality.
   */
  public static class EqualFunction {
    public static final String name = "equal";
    public static final CreatorFunction creatorFunction = (params -> {
      try {
        return CompareFunction.createCompareFunction(name, val -> {
          return val == 0;
        }, params);
      } catch (SolrException e) {
        if (params.length != 2) {
          throw e;
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
        }
        
        // This means that the Objects created by the AnalyticsValueStreams are not comparable, so use the .equals() method instead
        else if (paramA instanceof AnalyticsValue) {
          if (paramB instanceof AnalyticsValue) {
            return new ValueEqualFunction((AnalyticsValue)paramA,(AnalyticsValue)paramB);
          }
          return new StreamEqualFunction((AnalyticsValue)paramA,paramB);
        }
        if (paramB instanceof AnalyticsValue) {
          return new StreamEqualFunction((AnalyticsValue)paramB,paramA);
        }
      }
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires that at least 1 parameter be single-valued.");
    });
  }
  
  /**
   * A comparison function that tests whether the first parameter is greater than the second parameter
   */
  public static class GTFunction {
    public static final String name = "gt";
    public static final CreatorFunction creatorFunction = (params -> {
      return CompareFunction.createCompareFunction(name, val -> {
        return val > 0;
      }, params);
    });
  }

  /**
   * A comparison function that tests whether the first parameter is greater than or equal to the second parameter
   */
  public static class GTEFunction {
    public static final String name = "gte";
    public static final CreatorFunction creatorFunction = (params -> {
      return CompareFunction.createCompareFunction(name, val -> {
        return val >= 0;
      }, params);
    });
  }

  /**
   * A comparison function that tests whether the first parameter is less than the second parameter
   */
  public static class LTFunction {
    public static final String name = "lt";
    public static final CreatorFunction creatorFunction = (params -> {
      return CompareFunction.createCompareFunction(name, val -> {
        return val < 0;
      }, params);
    });
  }

  /**
   * A comparison function that tests whether the first parameter is less than or equal to the second parameter
   */
  public static class LTEFunction {
    public static final String name = "lte";
    public static final CreatorFunction creatorFunction = (params -> {
      return CompareFunction.createCompareFunction(name, val -> {
        return val <= 0;
      }, params);
    });
  }
  
  private static CompResultFunction reverse(CompResultFunction original) {
    return val -> original.apply(val*-1);
  }
}
@FunctionalInterface
interface CompResultFunction {
  public boolean apply(int compResult);
}
/**
 * A comparison function for two {@link DoubleValue}s.
 */
class CompareDoubleValueFunction extends AbstractBooleanValue {
  private final DoubleValue exprA;
  private final DoubleValue exprB;
  private final CompResultFunction comp;
  private final String name;
  private final String funcStr;
  private final ExpressionType funcType;
  
  public CompareDoubleValueFunction(String name, DoubleValue exprA, DoubleValue exprB, CompResultFunction comp) {
    this.name = name;
    this.exprA = exprA;
    this.exprB = exprB;
    this.comp = comp;
    this.funcStr = AnalyticsValueStream.createExpressionString(name,exprA,exprB);
    this.funcType = AnalyticsValueStream.determineMappingPhase(funcStr,exprA,exprB);
  }

  private boolean exists = false;
  @Override
  public boolean getBoolean() {
    double valueA = exprA.getDouble();
    double valueB = exprB.getDouble();
    exists = exprA.exists() && exprB.exists();
    return exists ? comp.apply(Double.compare(valueA,valueB)) : false;
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
 * A comparison function for a {@link DoubleValue} and a {@link DoubleValueStream}.
 */
class CompareDoubleStreamFunction extends AbstractBooleanValueStream {
  private final DoubleValue baseExpr;
  private final DoubleValueStream compExpr;
  private final CompResultFunction comp;
  private final String name;
  private final String funcStr;
  private final ExpressionType funcType;
  
  public CompareDoubleStreamFunction(String name, DoubleValue baseExpr, DoubleValueStream compExpr, CompResultFunction comp) throws SolrException {
    this.name = name;
    this.baseExpr = baseExpr;
    this.compExpr = compExpr;
    this.comp = comp;
    this.funcStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(funcStr,baseExpr,compExpr);
  }

  @Override
  public void streamBooleans(BooleanConsumer cons) {
    double baseValue = baseExpr.getDouble();
    if (baseExpr.exists()) {
      compExpr.streamDoubles(compValue -> cons.accept(comp.apply(Double.compare(baseValue,compValue))));
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
 * A comparison function for two {@link DateValue}s.
 */
class CompareDateValueFunction extends AbstractBooleanValue {
  private final DateValue exprA;
  private final DateValue exprB;
  private final CompResultFunction comp;
  private final String name;
  private final String funcStr;
  private final ExpressionType funcType;
  
  public CompareDateValueFunction(String name, DateValue exprA, DateValue exprB, CompResultFunction comp) {
    this.name = name;
    this.exprA = exprA;
    this.exprB = exprB;
    this.comp = comp;
    this.funcStr = AnalyticsValueStream.createExpressionString(name,exprA,exprB);
    this.funcType = AnalyticsValueStream.determineMappingPhase(funcStr,exprA,exprB);
  }

  private boolean exists = false;
  @Override
  public boolean getBoolean() {
    long valueA = exprA.getLong();
    long valueB = exprB.getLong();
    exists = exprA.exists() && exprB.exists();
    return exists ? comp.apply(Long.compare(valueA,valueB)) : false;
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
 * A comparison function for a {@link DateValue} and a {@link DateValueStream}.
 */
class CompareDateStreamFunction extends AbstractBooleanValueStream {
  private final DateValue baseExpr;
  private final DateValueStream compExpr;
  private final CompResultFunction comp;
  private final String name;
  private final String funcStr;
  private final ExpressionType funcType;
  
  public CompareDateStreamFunction(String name, DateValue baseExpr, DateValueStream compExpr, CompResultFunction comp) throws SolrException {
    this.name = name;
    this.baseExpr = baseExpr;
    this.compExpr = compExpr;
    this.comp = comp;
    this.funcStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(funcStr,baseExpr,compExpr);
  }

  @Override
  public void streamBooleans(BooleanConsumer cons) {
    long baseValue = baseExpr.getLong();
    if (baseExpr.exists()) {
      compExpr.streamLongs(compValue -> cons.accept(comp.apply(Long.compare(baseValue,compValue))));
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
 * A comparison function for two {@link StringValue}s.
 */
class CompareStringValueFunction extends AbstractBooleanValue {
  private final StringValue exprA;
  private final StringValue exprB;
  private final CompResultFunction comp;
  private final String name;
  private final String funcStr;
  private final ExpressionType funcType;
  
  public CompareStringValueFunction(String name, StringValue exprA, StringValue exprB, CompResultFunction comp) {
    this.name = name;
    this.exprA = exprA;
    this.exprB = exprB;
    this.comp = comp;
    this.funcStr = AnalyticsValueStream.createExpressionString(name,exprA,exprB);
    this.funcType = AnalyticsValueStream.determineMappingPhase(funcStr,exprA,exprB);
  }

  private boolean exists = false;
  @Override
  public boolean getBoolean() {
    String valueA = exprA.toString();
    String valueB = exprB.toString();
    exists = exprA.exists() && exprB.exists();
    return exists ? comp.apply(valueA.compareTo(valueB)) : false;
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
 * A comparison function for a {@link StringValue} and a {@link StringValueStream}.
 */
class CompareStringStreamFunction extends AbstractBooleanValueStream {
  private final StringValue baseExpr;
  private final StringValueStream compExpr;
  private final CompResultFunction comp;
  private final String name;
  private final String funcStr;
  private final ExpressionType funcType;
  
  public CompareStringStreamFunction(String name, StringValue baseExpr, StringValueStream compExpr, CompResultFunction comp) throws SolrException {
    this.name = name;
    this.baseExpr = baseExpr;
    this.compExpr = compExpr;
    this.comp = comp;
    this.funcStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(funcStr,baseExpr,compExpr);
  }

  @Override
  public void streamBooleans(BooleanConsumer cons) {
    String baseValue = baseExpr.toString();
    if (baseExpr.exists()) {
      compExpr.streamStrings(compValue -> cons.accept(comp.apply(baseValue.compareTo(compValue))));
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
 * An equal function for two {@link BooleanValue}s.
 */
class BooleanValueEqualFunction extends AbstractBooleanValue {
  private final BooleanValue exprA;
  private final BooleanValue exprB;
  public static final String name = CompareFunction.EqualFunction.name;
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
  public static final String name = CompareFunction.EqualFunction.name;
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
  public static final String name = CompareFunction.EqualFunction.name;
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
  public static final String name = CompareFunction.EqualFunction.name;
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