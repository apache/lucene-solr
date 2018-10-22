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
import java.util.Random;

import org.apache.commons.math3.distribution.EnumeratedIntegerDistribution;
import org.apache.commons.math3.util.MathArrays;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class MarkovChainEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public MarkovChainEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    if(2 < containedEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting no more then two parameters but found %d",expression,containedEvaluators.size()));
    }
  }

  @Override
  public Object doWork(Object... values) throws IOException{

    int state = -1;

    if(values.length == 2) {
      state = ((Number)values[1]).intValue();
    }

    if(values[0] instanceof Matrix) {
      Matrix matrix = (Matrix) values[0];
      return new MarkovChain(matrix, state);
    } else {
      throw new IOException("matrix parameter expected for markovChain function");
    }
  }

  public static class MarkovChain {

    private int state;
    private EnumeratedIntegerDistribution[] distributions;

    public MarkovChain(Matrix matrix, int state) throws IOException {
      double[][] data = matrix.getData();

      if(data.length != data[0].length) {
        throw new IOException("markovChain must be initialized with a square matrix.");
      }

      this.distributions = new EnumeratedIntegerDistribution[data.length];

      if(state > -1) {
        this.state = state;
      } else {
        this.state = new Random().nextInt(data.length);
      }

      for(int i=0; i<data.length; i++) {
        double[] probabilities = data[i];

        //Create the states array needed by the enumerated distribution
        int[] states = MathArrays.sequence(data.length, 0, 1);
        distributions[i] = new EnumeratedIntegerDistribution(states, probabilities);
      }
    }

    public Number sample() {
      this.state = distributions[this.state].sample();
      return this.state;
    }

    public int[] sample(int size) {
      int[] sample = new int[size];
      for(int i=0; i<size; i++) {
        sample[i] = sample().intValue();
      }

      return sample;
    }
  }
}
