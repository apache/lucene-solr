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

import org.apache.commons.math3.stat.inference.TTest;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.params.StreamParams;

public class PairedTTestEvaluator extends RecursiveNumericListEvaluator implements TwoValueWorker {
  protected static final long serialVersionUID = 1L;

  public PairedTTestEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    if(containedEvaluators.size() != 2){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting two parameters but found %d",expression,containedEvaluators.size()));
    }
  }

  @Override
  public Object doWork(Object value1, Object value2) throws IOException {

    TTest tTest = new TTest();
    Tuple tuple = new Tuple();
    if(value1 instanceof List) {
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

        double tstat = tTest.pairedT(samples1, samples2);
        double pval = tTest.pairedTTest(samples1, samples2);
        tuple.put("t-statistic", tstat);
        tuple.put(StreamParams.P_VALUE, pval);
        return tuple;
      } else {
        throw new IOException("Second parameter for pairedTtest must be a double array");
      }
    } else {
      throw new IOException("First parameter for pairedTtest must be a double array");
    }
  }
}
