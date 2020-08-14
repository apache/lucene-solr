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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class KolmogorovSmirnovEvaluator extends RecursiveObjectEvaluator implements TwoValueWorker {

  private static final long serialVersionUID = 1;

  public KolmogorovSmirnovEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
  }
  
  @Override
  public Object doWork(Object first, Object second) throws IOException{
    if(null == first || (first instanceof List<?> && ((List<?>) first).stream().anyMatch(item -> null == item))){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - null found for the first value",toExpression(constructingFactory)));
    }
    if(null == second || (second instanceof List<?> && ((List<?>) second).stream().anyMatch(item -> null == item))){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - null found for the second value",toExpression(constructingFactory)));
    }
    if(!(second instanceof List<?>) || ((List<?>) second).stream().anyMatch(item -> !(item instanceof Number))){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - found type %s for the second value, expecting a List of numbers",toExpression(constructingFactory), first.getClass().getSimpleName()));
    }
    
    KolmogorovSmirnovTest ks = new KolmogorovSmirnovTest();
    double[] data = ((List<?>)second).stream().mapToDouble(item -> ((Number)item).doubleValue()).toArray();
    
    if(first instanceof RealDistribution){
      RealDistribution realDistribution = (RealDistribution)first;

      Map<String,Double> m = new HashMap<>();
      m.put("p-value", ks.kolmogorovSmirnovTest(realDistribution, data));
      m.put("d-statistic", ks.kolmogorovSmirnovStatistic(realDistribution, data));
      return new Tuple(m);
    }
    else if(first instanceof List<?> && ((List<?>) first).stream().noneMatch(item -> !(item instanceof Number))){
      double[] data2 = ((List<?>)first).stream().mapToDouble(item -> ((Number)item).doubleValue()).toArray();
      
      Map<String,Double> m = new HashMap<>();
      m.put("d-statistic", ks.kolmogorovSmirnovTest(data, data2));
      return new Tuple(m);
    }
    else{
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - found type %s for the first value, expecting a RealDistribution or list of numbers",toExpression(constructingFactory), first.getClass().getSimpleName()));
    }
  }
}