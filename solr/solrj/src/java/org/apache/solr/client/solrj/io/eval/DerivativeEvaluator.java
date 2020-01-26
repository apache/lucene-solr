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

import org.apache.commons.math3.analysis.DifferentiableUnivariateFunction;
import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.commons.math3.analysis.interpolation.AkimaSplineInterpolator;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class DerivativeEvaluator extends RecursiveObjectEvaluator implements OneValueWorker {
  protected static final long serialVersionUID = 1L;

  public DerivativeEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  public Object doWork(Object value) throws IOException {
    if (null == value) {
      throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - null found for the first value", toExpression(constructingFactory)));
    }

    if (!(value instanceof VectorFunction)) {
      throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the first value, expecting a FunctionVector", toExpression(constructingFactory), value.getClass().getSimpleName()));
    }

    VectorFunction vectorFunction = (VectorFunction) value;

    DifferentiableUnivariateFunction func = null;
    double[] x = (double[])vectorFunction.getFromContext("x");

    if(!(vectorFunction.getFunction() instanceof DifferentiableUnivariateFunction)) {
      double[] y = (double[])vectorFunction.getFromContext("y");
      func = new AkimaSplineInterpolator().interpolate(x, y);
    } else {
      func = (DifferentiableUnivariateFunction) vectorFunction.getFunction();
    }

    UnivariateFunction derfunc = func.derivative();
    double[] dvalues = new double[x.length];
    for(int i=0; i<x.length; i++) {
      dvalues[i] = derfunc.value(x[i]);
    }

    VectorFunction vf = new VectorFunction(derfunc, dvalues);
    vf.addToContext("x", x);
    vf.addToContext("y", dvalues);

    return vf;
  }
}
