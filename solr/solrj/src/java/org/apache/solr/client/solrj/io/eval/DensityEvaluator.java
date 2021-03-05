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

import org.apache.commons.math3.distribution.MultivariateRealDistribution;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class DensityEvaluator extends RecursiveObjectEvaluator implements TwoValueWorker {
  protected static final long serialVersionUID = 1L;

  public DensityEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  public Object doWork(Object first, Object second) throws IOException{

    if (!(first instanceof MultivariateRealDistribution)) {
      throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the first value, expecting a MultiVariateRealDistribution for density", toExpression(constructingFactory), first.getClass().getSimpleName()));
    }
    if (!(second instanceof List)) {
      throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the second value, expecting a numeric array.", toExpression(constructingFactory), first.getClass().getSimpleName()));
    }

    MultivariateRealDistribution multivariateRealDistribution = (MultivariateRealDistribution) first;
    @SuppressWarnings({"unchecked"})
    List<Number> nums = (List<Number>) second;

    double[] vec = new double[nums.size()];

    for(int i=0; i<vec.length; i++) {
      vec[i] = nums.get(i).doubleValue();
    }

    return multivariateRealDistribution.density(vec);
  }
}
