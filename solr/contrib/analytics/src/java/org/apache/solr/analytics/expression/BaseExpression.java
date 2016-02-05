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

import org.apache.solr.analytics.statistics.StatsCollector;


/**
 * <code>BaseExpression</code> returns the value returned by the {@link StatsCollector} for the specified stat.
 */
public class BaseExpression extends Expression {
  protected final StatsCollector statsCollector;
  protected final String stat;
  
  public BaseExpression(StatsCollector statsCollector, String stat) {
    this.statsCollector = statsCollector;
    this.stat = stat;
  }
  
  public Comparable getValue() {
    if(statsCollector.getStatsList().contains(stat)) {
      return statsCollector.getStat(stat);
    }
    return null;
  }
}
/**
 * <code>ConstantStringExpression</code> returns the specified constant double.
 */
class ConstantNumberExpression extends Expression {
  protected final Double constant;
  
  public ConstantNumberExpression(double d) {
    constant = new Double(d);
  }
  
  public Comparable getValue() {
    return constant;
  }
}
/**
 * <code>ConstantStringExpression</code> returns the specified constant date.
 */
class ConstantDateExpression extends Expression {
  protected final Date constant;
  
  public ConstantDateExpression(Date date) {
    constant = date;
  }
  
  public ConstantDateExpression(Long date) {
    constant = new Date(date);
  }
  
  public Comparable getValue() {
    return constant;
  }
}
/**
 * <code>ConstantStringExpression</code> returns the specified constant string.
 */
class ConstantStringExpression extends Expression {
  protected final String constant;
  
  public ConstantStringExpression(String str) {
    constant = str;
  }
  
  public Comparable getValue() {
    return constant;
  }
}
