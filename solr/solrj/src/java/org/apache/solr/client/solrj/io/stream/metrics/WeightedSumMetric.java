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
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class WeightedSumMetric extends Metric {

  public static final String FUNC = "wsum";
  private String valueCol;
  private String countCol;
  private List<Part> parts;

  public WeightedSumMetric(String valueCol, String countCol) {
    init(valueCol, countCol, false);
  }

  public WeightedSumMetric(String valueCol, String countCol, boolean outputLong) {
    init(valueCol, countCol, outputLong);
  }

  public WeightedSumMetric(StreamExpression expression, StreamFactory factory) throws IOException {
    // grab all parameters out
    String functionName = expression.getFunctionName();
    if (!FUNC.equals(functionName)) {
      throw new IOException("Expected '" + FUNC + "' function but found " + functionName);
    }
    String valueCol = factory.getValueOperand(expression, 0);
    String countCol = factory.getValueOperand(expression, 1);
    String outputLong = factory.getValueOperand(expression, 2);

    // validate expression contains only what we want.
    if (null == valueCol) {
      throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - expected %s(valueCol,countCol)", expression, FUNC));
    }

    boolean ol = false;
    if (outputLong != null) {
      ol = Boolean.parseBoolean(outputLong);
    }

    init(valueCol, countCol, ol);
  }

  private void init(String valueCol, String countCol, boolean outputLong) {
    this.valueCol = valueCol;
    this.countCol = countCol != null ? countCol : "count(*)";
    this.outputLong = outputLong;
    setFunctionName(FUNC);
    setIdentifier(FUNC, "(", valueCol, ", " + countCol + ", " + outputLong + ")");
  }

  public void update(Tuple tuple) {
    Object c = tuple.get(countCol);
    Object o = tuple.get(valueCol);
    if (c instanceof Number && o instanceof Number) {
      if (parts == null) {
        parts = new LinkedList<>();
      }
      Number count = (Number) c;
      Number value = (Number) o;
      parts.add(new Part(count.longValue(), value.doubleValue()));
    }
  }

  public Metric newInstance() {
    return new WeightedSumMetric(valueCol, countCol, outputLong);
  }

  public String[] getColumns() {
    return new String[]{valueCol, countCol};
  }

  public Number getValue() {
    long total = sumCounts();
    double wavg = 0d;
    if (total > 0L) {
      for (Part next : parts) {
        wavg += next.weighted(total);
      }
    }
    return outputLong ? Math.round(wavg) : wavg;
  }

  private long sumCounts() {
    long total = 0L;
    if (parts != null) {
      for (Part next : parts) {
        total += next.count;
      }
    }
    return total;
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return new StreamExpression(getFunctionName()).withParameter(valueCol).withParameter(countCol).withParameter(Boolean.toString(outputLong));
  }

  private static final class Part {
    private final double value;
    private final long count;

    Part(long count, double value) {
      this.count = count;
      this.value = value;
    }

    private double weighted(final long total) {
      return ((double) count / total) * value;
    }
  }
}
