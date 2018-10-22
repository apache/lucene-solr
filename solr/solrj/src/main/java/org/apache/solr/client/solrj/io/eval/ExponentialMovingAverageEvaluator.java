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

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;


public class ExponentialMovingAverageEvaluator extends RecursiveNumericEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public ExponentialMovingAverageEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
    if (!(2 == containedEvaluators.size() ||  containedEvaluators.size() == 3)){
        throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting two or three values but found %d",expression, containedEvaluators.size()));
    }
  }

  @Override
  public Object doWork(Object... values) throws IOException {
    if (!(2 == values.length ||  values.length == 3)){
      throw new IOException(String.format(Locale.ROOT,"%s(...) only works with 2 or 3 values but %d were provided", constructingFactory.getFunctionName(getClass()), values.length));
    }
    List<?> observations = (List<?> )values[0];
    Number window = (Number)values[1];
    Number alpha;
    if(2 == values.length){
      if(!(observations instanceof List<?>)){
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the first value, expecting a List", toExpression(constructingFactory), values[0].getClass().getSimpleName()));
      }
      if(!(observations.size() > 1)){
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found list size of %s for the first value, expecting a List of size > 0.", toExpression(constructingFactory), observations.size()));
      }
      if(!(window instanceof Number)){
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the second value, expecting a Number", toExpression(constructingFactory), values[1].getClass().getSimpleName()));
      }
      if (window.doubleValue() >  observations.size()) {
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found a window size of %s for the second value, the first value has a List size of %s, expecting a window value smaller or equal to the List size", toExpression(constructingFactory), window.intValue(), observations.size() ));
      }
    }

    if(3 == values.length){
      alpha = (Number) values[2];
      if(!(alpha instanceof Number)){
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the third value, expecting a Number", toExpression(constructingFactory), values[2].getClass().getSimpleName()));
      }
      if (!(alpha.doubleValue() >= 0 && alpha.doubleValue() <= 1.0)) {
        throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - out of range, found %s for the third value, expecting a range between 0 and 1.0",toExpression(constructingFactory), alpha.doubleValue()));
      }
    }else {
         alpha = 2.0/(window.doubleValue() + 1.0);
    }

    List<Number> sequence = new ArrayList<>();
    DescriptiveStatistics slider = new DescriptiveStatistics(window.intValue());
    Number lastValue = 0;
    for(Object value : observations) {
      slider.addValue(((Number) value).doubleValue());
      if (slider.getN() == window.intValue()) {
        lastValue = slider.getMean();
        break;
      }
    }

    sequence.add(lastValue);
    int i=0;

    for(Object value : observations) {
      if(i >= window.intValue()) {
        Number val = (alpha.doubleValue() * (((Number) value).doubleValue() - lastValue.doubleValue())+lastValue.doubleValue());
        sequence.add(val);
        lastValue = val;
      }
      ++i;
    }
    return sequence;
  }
}
