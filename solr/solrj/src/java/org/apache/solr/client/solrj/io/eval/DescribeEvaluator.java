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
package org.apache.solr.client.solrj.io.eval;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class DescribeEvaluator extends RecursiveNumericEvaluator implements OneValueWorker {
  protected static final long serialVersionUID = 1L;
  
  public DescribeEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
    
    if(1 != containedEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting exactly one value but found %d",expression,containedEvaluators.size()));
    }
  }
  
  @Override
  public Object doWork(Object value) throws IOException {
    
    if(!(value instanceof List<?>)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a numeric list but found %s", toExpression(constructingFactory), value.getClass().getSimpleName()));
    }
    
    // we know each value is a BigDecimal or a list of BigDecimals
    DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
    ((List<?>)value).stream().mapToDouble(innerValue -> ((Number)innerValue).doubleValue()).forEach(innerValue -> descriptiveStatistics.addValue(innerValue));

    Tuple tuple = new Tuple();
    tuple.put("max", descriptiveStatistics.getMax());
    tuple.put("mean", descriptiveStatistics.getMean());
    tuple.put("min", descriptiveStatistics.getMin());
    tuple.put("stdev", descriptiveStatistics.getStandardDeviation());
    tuple.put("sum", descriptiveStatistics.getSum());
    tuple.put("N", descriptiveStatistics.getN());
    tuple.put("var", descriptiveStatistics.getVariance());
    tuple.put("kurtosis", descriptiveStatistics.getKurtosis());
    tuple.put("skewness", descriptiveStatistics.getSkewness());
    tuple.put("popVar", descriptiveStatistics.getPopulationVariance());
    tuple.put("geometricMean", descriptiveStatistics.getGeometricMean());
    tuple.put("sumsq", descriptiveStatistics.getSumsq());

    return tuple;
  }  
}
