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

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class IFFTEvaluator extends RecursiveObjectEvaluator implements OneValueWorker {
  protected static final long serialVersionUID = 1L;

  public IFFTEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
    if(containedEvaluators.size() < 1){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at least one value but found %d",expression,containedEvaluators.size()));
    }

  }

  @Override
  public Object doWork(Object v) throws IOException {

    if(v instanceof Matrix) {

      Matrix matrix = (Matrix)v;
      double[][] data = matrix.getData();
      double[] real = data[0];
      double[] imaginary = data[1];
      Complex[] complex = new Complex[real.length];

      for (int i = 0; i < real.length; ++i) {
       complex[i] = new Complex(real[i], imaginary[i]);
      }

      FastFourierTransformer fastFourierTransformer = new FastFourierTransformer(DftNormalization.STANDARD);
      Complex[] result  = fastFourierTransformer.transform(complex, TransformType.INVERSE);

      List<Number> realResult = new ArrayList<>();
      for (int i = 0; i < result.length; ++i) {
        realResult.add(result[i].getReal());
      }

      return realResult;
    } else {
      throw new IOException("ifft function requires a matrix as a parameter");
    }
  }
}