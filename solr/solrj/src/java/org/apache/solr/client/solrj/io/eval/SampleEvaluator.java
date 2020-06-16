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
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.List;

import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.MultivariateRealDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class SampleEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {

  private static final long serialVersionUID = 1;

  public SampleEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
  }
  
  @Override
  public Object doWork(Object ... objects) throws IOException{
    if(objects.length < 1){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - null found for the first value",toExpression(constructingFactory)));
    }

    Object first = objects[0];

    if(!(first instanceof MultivariateRealDistribution) && !(first instanceof RealDistribution) && !(first instanceof IntegerDistribution) && !(first instanceof MarkovChainEvaluator.MarkovChain)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - found type %s for the first value, expecting a Markov Chain, Real or Integer Distribution",toExpression(constructingFactory), first.getClass().getSimpleName()));
    }

    Object second = null;
    if(objects.length > 1) {
      second = objects[1];
    }

    if(first instanceof MarkovChainEvaluator.MarkovChain) {
      MarkovChainEvaluator.MarkovChain markovChain = (MarkovChainEvaluator.MarkovChain)first;
      if(second != null) {
        return Arrays.stream(markovChain.sample(((Number) second).intValue())).mapToObj(item -> item).collect(Collectors.toList());
      } else {
        return markovChain.sample();
      }
    } else if (first instanceof RealDistribution) {
      RealDistribution realDistribution = (RealDistribution) first;
      if (second != null) {
        return Arrays.stream(realDistribution.sample(((Number) second).intValue())).mapToObj(item -> item).collect(Collectors.toList());
      } else {
        return realDistribution.sample();
      }
    }else if(first instanceof MultivariateNormalDistribution) {
      if(second != null) {
        MultivariateNormalDistribution multivariateNormalDistribution = (MultivariateNormalDistribution)first;
        int size = ((Number)second).intValue();
        double[][] samples = new double[size][];
        for(int i=0; i<size; ++i) {
          samples[i] =  multivariateNormalDistribution.sample();
        }

        return new Matrix(samples);
      } else {
        MultivariateNormalDistribution multivariateNormalDistribution = (MultivariateNormalDistribution)first;
        double[] sample = multivariateNormalDistribution.sample();
        List<Number> sampleList = new ArrayList<>(sample.length);
        for(int i=0; i<sample.length; i++) {
          sampleList.add(sample[i]);
        }
        return sampleList;
      }
    } else {
      IntegerDistribution integerDistribution = (IntegerDistribution) first;
      if(second != null) {
        return Arrays.stream(integerDistribution.sample(((Number) second).intValue())).mapToObj(item -> item).collect(Collectors.toList());
      } else {
        return integerDistribution.sample();
      }
    }
  }
}