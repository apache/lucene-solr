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
package org.apache.solr.client.solrj.io.stream.metrics;

import java.io.IOException;
import java.util.Locale;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class MinMetric extends Metric {
  
  private long longMin = Long.MAX_VALUE;
  private double doubleMin = Double.MAX_VALUE;
  private String columnName;

  public MinMetric(String columnName){
    init("min", columnName);
  }

  public MinMetric(StreamExpression expression, StreamFactory factory) throws IOException{
    // grab all parameters out
    String functionName = expression.getFunctionName();
    String columnName = factory.getValueOperand(expression, 0);
    
    // validate expression contains only what we want.
    if(null == columnName){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expected %s(columnName)", expression, functionName));
    }
    if(1 != expression.getParameters().size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }
    
    init(functionName, columnName);    
  }
  
  private void init(String functionName, String columnName){
    this.columnName = columnName;
    setFunctionName(functionName);
    setIdentifier(functionName, "(", columnName, ")");
  }

  public String[] getColumns() {
    return new String[]{columnName};
  }

  public Number getValue() {
    if(longMin == Long.MAX_VALUE) {
      return doubleMin;
    } else {
      return longMin;
    }
  }

  public void update(Tuple tuple) {
    Object o = tuple.get(columnName);
    if(o instanceof Double) {
      double d = (double) o;
      if (d < doubleMin) {
        doubleMin = d;
      }
    } else if(o instanceof Float) {
      Float f = (Float) o;
      double d = f.doubleValue();
      if (d < doubleMin) {
        doubleMin = d;
      }
    } else if(o instanceof Integer) {
      Integer i = (Integer)o;
      long l = i.longValue();
      if(l < longMin) {
        longMin = l;
      }
    } else if(o instanceof Long) {
      long l = (long)o;
      if(l < longMin) {
        longMin = l;
      }
    }
  }

  public Metric newInstance() {
    return new MinMetric(columnName);
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return new StreamExpression(getFunctionName()).withParameter(columnName);
  }
}