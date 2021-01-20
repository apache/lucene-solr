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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.commons.math3.stat.inference.MannWhitneyUTest;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.params.StreamParams;


public class MannWhitneyUEvaluator extends RecursiveNumericListEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public MannWhitneyUEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);

    if(containedEvaluators.size() < 1){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at least one value but found %d",expression,containedEvaluators.size()));
    }
  }

  @Override
  public Object doWork(Object... values) throws IOException {
    @SuppressWarnings({"unchecked"})
    List<double[]> mannWhitneyUInput = Arrays.stream(values)
        .map(value -> ((List<Number>) value).stream().mapToDouble(Number::doubleValue).toArray())
        .collect(Collectors.toList());
    if(mannWhitneyUInput.size() == 2) {
      MannWhitneyUTest mannwhitneyutest = new MannWhitneyUTest();
      double u = mannwhitneyutest.mannWhitneyU(mannWhitneyUInput.get(0), mannWhitneyUInput.get(1));
      double p = mannwhitneyutest.mannWhitneyUTest(mannWhitneyUInput.get(0), mannWhitneyUInput.get(1));
      Tuple tuple = new Tuple();
      tuple.put("u-statistic", u);
      tuple.put(StreamParams.P_VALUE, p);
      return tuple;
    }else{
      throw new IOException(String.format(Locale.ROOT,"%s(...) only works with a list of 2 arrays but a list of %d array(s) was provided.", constructingFactory.getFunctionName(getClass()), mannWhitneyUInput.size()));
    }
  }
}