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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.apache.commons.math3.stat.Frequency;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class FrequencyTableEvaluator extends RecursiveNumericEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public FrequencyTableEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    if(containedEvaluators.size() < 1){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at least one value but found %d",expression,containedEvaluators.size()));
    }
  }

  @Override
  public Object doWork(Object... values) throws IOException {
    if(Arrays.stream(values).anyMatch(item -> null == item)){
      return null;
    }

    List<?> sourceValues;

    if(values.length == 1){
      sourceValues = values[0] instanceof List<?> ? (List<?>)values[0] : Arrays.asList(values[0]);
    }
    else
    {
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at least one value but found %d",toExpression(constructingFactory),containedEvaluators.size()));
    }

    Frequency frequency = new Frequency();

    for(Object o : sourceValues) {
      Number number = (Number)o;
      frequency.addValue(number.longValue());
    }

    List<Tuple> histogramBins = new ArrayList<>();

    @SuppressWarnings({"rawtypes"})
    Iterator iterator = frequency.valuesIterator();

    while(iterator.hasNext()){
      Long value = (Long)iterator.next();
      Tuple tuple = new Tuple();
      tuple.put("value", value.longValue());
      tuple.put("count", frequency.getCount(value));
      tuple.put("cumFreq", frequency.getCumFreq(value));
      tuple.put("cumPct", frequency.getCumPct(value));
      tuple.put("pct", frequency.getPct(value));
      histogramBins.add(tuple);
    }
    return histogramBins;
  }
}
