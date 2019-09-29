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
import java.util.Locale;

import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.commons.math3.analysis.integration.RombergIntegrator;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class IntegrateEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public IntegrateEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  public Object doWork(Object... values) throws IOException {

    if(values.length > 3) {
      throw new IOException("The integrate function requires at most 3 parameters");
    }

    if (!(values[0] instanceof VectorFunction)) {
      throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the first value, expecting a FunctionVector", toExpression(constructingFactory), values[0].getClass().getSimpleName()));
    }

    VectorFunction vectorFunction = (VectorFunction) values[0];
    if (!(vectorFunction.getFunction() instanceof UnivariateFunction)) {
      throw new IOException("Cannot evaluate integral from parameter.");
    }

    UnivariateFunction func = (UnivariateFunction) vectorFunction.getFunction();

    if(values.length == 3) {


      Number min = null;
      Number max = null;

      if (values[1] instanceof Number) {
        min = (Number) values[1];
      } else {
        throw new IOException("The second parameter of the integrate function must be a number");
      }

      if (values[2] instanceof Number) {
        max = (Number) values[2];
      } else {
        throw new IOException("The third parameter of the integrate function must be a number");
      }

      RombergIntegrator rombergIntegrator = new RombergIntegrator();
      return rombergIntegrator.integrate(5000, func, min.doubleValue(), max.doubleValue());
    } else {
      RombergIntegrator integrator = new RombergIntegrator();

      double[] x = (double[])vectorFunction.getFromContext("x");
      double[] y = (double[])vectorFunction.getFromContext("y");
      ArrayList<Number> out = new ArrayList();
      out.add(0);
      for(int i=1; i<x.length; i++) {
        out.add(integrator.integrate(5000, func, x[0], x[i]));
      }

      return out;

    }
  }
}
