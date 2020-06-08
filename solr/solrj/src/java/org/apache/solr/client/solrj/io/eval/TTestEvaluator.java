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

import org.apache.commons.math3.stat.inference.TTest;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class TTestEvaluator extends RecursiveNumericEvaluator implements TwoValueWorker {
  protected static final long serialVersionUID = 1L;

  public TTestEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    if(containedEvaluators.size() != 2){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting two parameters but found %d",expression,containedEvaluators.size()));
    }
  }

  @Override
  public Object doWork(Object value1, Object value2) throws IOException {

    TTest tTest = new TTest();
    @SuppressWarnings({"rawtypes"})
    Map map = new HashMap();
    Tuple tuple = new Tuple(map);
    if(value1 instanceof Number) {
      double mean = ((Number) value1).doubleValue();

      if(value2 instanceof List) {
        @SuppressWarnings({"unchecked"})
        List<Number> values = (List<Number>) value2;
        double[] samples = new double[values.size()];
        for (int i = 0; i < samples.length; i++) {
          samples[i] = values.get(i).doubleValue();
        }

        double tstat = tTest.t(mean, samples);
        double pval = tTest.tTest(mean, samples);

        tuple.put("t-statistic", tstat);
        tuple.put("p-value", pval);
        return tuple;
      } else {
        throw new IOException("Second parameter for ttest must be a double array");
      }
    } else if(value1 instanceof List) {
      @SuppressWarnings({"unchecked"})
      List<Number> values1 = (List<Number>)value1;

      double[] samples1 = new double[values1.size()];

      for(int i=0; i< samples1.length; i++) {
        samples1[i] = values1.get(i).doubleValue();
      }

      if(value2 instanceof List) {
        @SuppressWarnings({"unchecked"})
        List<Number> values2 = (List<Number>) value2;
        double[] samples2 = new double[values2.size()];

        for (int i = 0; i < samples2.length; i++) {
          samples2[i] = values2.get(i).doubleValue();
        }

        double tstat = tTest.t(samples1, samples2);
        double pval = tTest.tTest(samples1, samples2);
        tuple.put("t-statistic", tstat);
        tuple.put("p-value", pval);
        return tuple;
      } else {
        throw new IOException("Second parameter for ttest must be a double array");
      }
    } else {
      throw new IOException("First parameter for ttest must be either a double our double array");
    }
  }
}
