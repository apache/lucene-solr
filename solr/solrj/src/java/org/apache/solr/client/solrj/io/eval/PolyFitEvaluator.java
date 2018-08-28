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

import org.apache.commons.math3.analysis.polynomials.PolynomialFunction;
import org.apache.commons.math3.fitting.PolynomialCurveFitter;

import org.apache.commons.math3.fitting.WeightedObservedPoints;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class PolyFitEvaluator extends RecursiveNumericEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public PolyFitEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  public Object doWork(Object... objects) throws IOException{

    if(objects.length > 3) {
      throw new IOException("polyfit function takes a maximum of 3 arguments.");
    }

    Object first = objects[0];

    double[] x = null;
    double[] y = null;
    int degree = 3;

    if(objects.length == 1) {
      //Only the y values passed

      y = ((List) first).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray();
      x = new double[y.length];
      for(int i=0; i<y.length; i++) {
        x[i] = i;
      }

    } else if(objects.length == 3) {
      // x, y and degree passed

      Object second = objects[1];
      x = ((List) first).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray();
      y = ((List) second).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray();
      degree = ((Number)objects[2]).intValue();
    } else if(objects.length == 2) {
      if(objects[1] instanceof List) {
        // x and y passed
        Object second = objects[1];
        x = ((List) first).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray();
        y = ((List) second).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray();
      } else {
        // y and degree passed
        y = ((List) first).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray();
        x = new double[y.length];
        for(int i=0; i<y.length; i++) {
          x[i] = i;
        }

        degree = ((Number)objects[1]).intValue();
      }
    }

    PolynomialCurveFitter curveFitter = PolynomialCurveFitter.create(degree);
    WeightedObservedPoints points = new WeightedObservedPoints();
    for(int i=0; i<x.length; i++) {
      points.add(x[i], y[i]);
    }

    double[] coef = curveFitter.fit(points.toList());
    PolynomialFunction pf = new PolynomialFunction(coef);

    List list = new ArrayList();
    for(double xvalue : x) {
      double yvalue= pf.value(xvalue);
      list.add(yvalue);
    }


    VectorFunction vec = new VectorFunction(pf, list);
    vec.addToContext("x", x);
    vec.addToContext("y", y);

    return vec;
  }
}
