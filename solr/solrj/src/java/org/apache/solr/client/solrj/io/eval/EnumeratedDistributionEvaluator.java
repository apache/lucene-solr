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

import org.apache.commons.math3.distribution.EnumeratedIntegerDistribution;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class EnumeratedDistributionEvaluator extends RecursiveNumericEvaluator implements ManyValueWorker {

  private static final long serialVersionUID = 1;

  public EnumeratedDistributionEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
  }

  @Override
  public Object doWork(Object... values) throws IOException{
    if(values.length == 0){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - null found for the first value",toExpression(constructingFactory)));
    }

    if(values.length == 1) {
      @SuppressWarnings({"unchecked"})
      List<Number> first = (List<Number>)values[0];
      @SuppressWarnings({"unchecked", "rawtypes"})
      int[] samples = ((List) first).stream().mapToInt(value -> ((Number) value).intValue()).toArray();
      return new EnumeratedIntegerDistribution(samples);
    } else {
      @SuppressWarnings({"unchecked"})
      List<Number> first = (List<Number>)values[0];
      @SuppressWarnings({"unchecked"})
      List<Number> second = (List<Number>)values[1];
      @SuppressWarnings({"unchecked", "rawtypes"})
      int[] singletons = ((List) first).stream().mapToInt(value -> ((Number) value).intValue()).toArray();
      @SuppressWarnings({"unchecked", "rawtypes"})
      double[] probs = ((List) second).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray();
      return new EnumeratedIntegerDistribution(singletons, probs);
    }
  }
}