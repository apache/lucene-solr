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
import java.util.ArrayList;

import org.apache.commons.math3.analysis.interpolation.AkimaSplineInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class AkimaEvaluator extends RecursiveNumericEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public AkimaEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public Object doWork(Object... objects) throws IOException{

    Object first = objects[0];

    double[] x = null;
    double[] y = null;

    if(objects.length == 1) {
      //Only the y values passed
      y = ((List) first).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray();
      x = new double[y.length];
      for(int i=0; i<y.length; i++) {
        x[i] = i;
      }
    } else if(objects.length == 2) {
      Object second = objects[1];
      x = ((List) first).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray();
      y = ((List) second).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray();
    }

    AkimaSplineInterpolator interpolator = new AkimaSplineInterpolator();
    PolynomialSplineFunction spline = interpolator.interpolate(x, y);

    List<Number> list = new ArrayList<>();
    for(double xvalue : x) {
      list.add(spline.value(xvalue));
    }

    VectorFunction vec = new VectorFunction(spline, list);
    vec.addToContext("x", x);
    vec.addToContext("y", y);

    return vec;
  }

}
