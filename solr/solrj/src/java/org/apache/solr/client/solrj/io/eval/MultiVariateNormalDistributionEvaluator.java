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
import java.util.Locale;
import java.util.List;

import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class MultiVariateNormalDistributionEvaluator extends RecursiveObjectEvaluator implements TwoValueWorker {

  private static final long serialVersionUID = 1;

  public MultiVariateNormalDistributionEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
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

    @SuppressWarnings({"unchecked"})
    List<Number> means = (List<Number>)first;
    Matrix covar = (Matrix)second;

    double[] m = new double[means.size()];
    for(int i=0; i< m.length; i++) {
      m[i] = means.get(i).doubleValue();
    }

    return new MultivariateNormalDistribution(m, covar.getData());
  }
}