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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class PercentileEvaluator extends RecursiveNumericEvaluator implements TwoValueWorker {
  protected static final long serialVersionUID = 1L;
  
  public PercentileEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  public Object doWork(Object first, Object second) throws IOException{
    if(null == first){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - null found for the first value",toExpression(constructingFactory)));
    }
    if(null == second){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - null found for the second value",toExpression(constructingFactory)));
    }
    if(!(first instanceof List<?>)) {
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - found type %s for the first value, expecting a List",toExpression(constructingFactory), first.getClass().getSimpleName()));
    }
    if((second instanceof Number)) {
      Percentile percentile = new Percentile();
      percentile.setData(((List<?>) first).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray());
      return percentile.evaluate(((Number) second).doubleValue());
    } else if(second instanceof List){
      Percentile percentile = new Percentile();
      percentile.setData(((List<?>) first).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray());
      @SuppressWarnings({"unchecked"})
      List<Number> values = (List<Number>) second;
      List<Number> percentiles = new ArrayList<>();
      for(Number value : values) {
        percentiles.add(percentile.evaluate(value.doubleValue()));
      }

      return percentiles;
    } else {
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - found type %s for the second value, expecting a number or a numeric array",toExpression(constructingFactory), first.getClass().getSimpleName()));
    }
  }
  
}
