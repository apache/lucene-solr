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

public class MeanMetric extends Metric {
  // How'd the MeanMetric get to be so mean?
  // Maybe it was born with it.
  // Maybe it was mayba-mean.
  //
  // I'll see myself out.

  private String columnName;
  private double doubleSum;
  private long longSum;
  private long count;

  public MeanMetric(String columnName){
    init("avg", columnName, false);
  }

  public MeanMetric(String columnName, boolean outputLong){
    init("avg", columnName, outputLong);
  }

  public MeanMetric(StreamExpression expression, StreamFactory factory) throws IOException{
    // grab all parameters out
    String functionName = expression.getFunctionName();
    String columnName = factory.getValueOperand(expression, 0);
    String outputLong = factory.getValueOperand(expression, 1);


    // validate expression contains only what we want.
    if(null == columnName){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expected %s(columnName)", expression, functionName));
    }

    boolean ol = false;
    if(outputLong != null) {
      ol = Boolean.parseBoolean(outputLong);
    }
    
    init(functionName, columnName, ol);
  }
  
  private void init(String functionName, String columnName, boolean outputLong){
    this.columnName = columnName;
    this.outputLong = outputLong;
    setFunctionName(functionName);
    setIdentifier(functionName, "(", columnName, ")");
  }
  
  public void update(Tuple tuple) {
    ++count;
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
    return new MeanMetric(columnName, outputLong);
  }

  public String[] getColumns() {
    return new String[]{columnName};
  }

  public Number getValue() {
    double dcount = (double)count;
    if(longSum == 0) {
      return doubleSum/dcount;
    } else {
      double mean = longSum/dcount;
      if(outputLong) {
        return Math.round(mean);
      } else {
        return mean;
      }
    }
  }
  
  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return new StreamExpression(getFunctionName()).withParameter(columnName).withParameter(Boolean.toString(outputLong));
  }
}