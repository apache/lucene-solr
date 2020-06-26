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

public class PercentileMetric extends Metric {
  private long longMax = -Long.MIN_VALUE;
  private double doubleMax = -Double.MAX_VALUE;
  private String columnName;

  public PercentileMetric(String columnName, int percentile){

    init("per", columnName, percentile);
  }

  public PercentileMetric(StreamExpression expression, StreamFactory factory) throws IOException{
    // grab all parameters out
    String functionName = expression.getFunctionName();
    String columnName = factory.getValueOperand(expression, 0);
    int percentile = Integer.parseInt(factory.getValueOperand(expression, 1));

    // validate expression contains only what we want.
    if(null == columnName){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expected %s(columnName)", expression, functionName));
    }
    if(2 != expression.getParameters().size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }

    init(functionName, columnName, percentile);
  }

  private void init(String functionName, String columnName, int percentile){
    this.columnName = columnName;
    setFunctionName(functionName);
    setIdentifier(functionName, "(", columnName, ","+percentile, ")");
  }

  public Number getValue() {
    if(longMax == Long.MIN_VALUE) {
      return doubleMax;
    } else {
      return longMax;
    }
  }

  public String[] getColumns() {
    return new String[]{columnName};
  }

  public void update(Tuple tuple) {

  }

  public Metric newInstance() {
    return new MaxMetric(columnName);
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return new StreamExpression(getFunctionName()).withParameter(columnName);
  }
}