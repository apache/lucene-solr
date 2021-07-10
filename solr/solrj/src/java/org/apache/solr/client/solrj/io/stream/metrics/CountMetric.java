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

public class CountMetric extends Metric {
  private String columnName;
  private long count;
  private boolean isAllColumns;

  public CountMetric() {
    this("*");
  }

  public CountMetric(String columnName) {
    init("count", columnName);
  }

  public CountMetric(StreamExpression expression, StreamFactory factory) throws IOException{
    // grab all parameters out
    String functionName = expression.getFunctionName();
    String columnName = factory.getValueOperand(expression, 0);

    if(1 != expression.getParameters().size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }

    init(functionName, columnName);
  }

  public String[] getColumns() {
    if(isAllColumns()) {
      return new String[0];
    }
    return new String[]{columnName};
  }

  private void init(String functionName, String columnName){
    this.columnName = columnName;
    this.isAllColumns = "*".equals(this.columnName);
    this.outputLong = true;
    setFunctionName(functionName);
    setIdentifier(functionName, "(", columnName, ")");
  }

  private boolean isAllColumns() {
    return isAllColumns;
  }

  public void update(Tuple tuple) {
    if(isAllColumns() || tuple.get(columnName) != null) {
      ++count;
    }
  }

  public Long getValue() {
    return count;
  }

  public Metric newInstance() {
    return new CountMetric(columnName);
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return new StreamExpression(getFunctionName()).withParameter(columnName);
  }
}