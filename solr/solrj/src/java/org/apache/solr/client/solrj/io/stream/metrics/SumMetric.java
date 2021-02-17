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

public class SumMetric extends Metric {
  private String columnName;
  private double doubleSum;
  private long longSum;

  public SumMetric(String columnName){
    init("sum", columnName);
  }

  public SumMetric(StreamExpression expression, StreamFactory factory) throws IOException{
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

  public void update(Tuple tuple) {
    Object o = tuple.get(columnName);
    if(o instanceof Double) {
      Double d = (Double) o;
      doubleSum += d;
    } else if(o instanceof Float) {
      Float f = (Float) o;
      doubleSum += f.doubleValue();
    } else if(o instanceof Integer) {
      Integer i = (Integer)o;
      longSum += i.longValue();
    } else if (o instanceof Long) {
      Long l = (Long)o;
      longSum += l;
    }
  }

  public Metric newInstance() {
    return new SumMetric(columnName);
  }

  public Number getValue() {
    if(longSum == 0) {
      return doubleSum;
    } else {
      return longSum;
    }
  }
  
  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return new StreamExpression(getFunctionName()).withParameter(columnName);
  }
}