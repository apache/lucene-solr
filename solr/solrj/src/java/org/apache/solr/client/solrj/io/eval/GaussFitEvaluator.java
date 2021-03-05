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

import org.apache.commons.math3.analysis.function.Gaussian;
import org.apache.commons.math3.fitting.GaussianCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;
import org.apache.commons.math3.fitting.WeightedObservedPoint;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class GaussFitEvaluator extends RecursiveNumericEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public GaussFitEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Object doWork(Object... objects) throws IOException{

    if(objects.length >= 3) {
      throw new IOException("gaussfit function takes a maximum of 2 arguments.");
    }

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
      // x and y passed
      Object second = objects[1];
      x = ((List) first).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray();
      y = ((List) second).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray();


    }

    GaussianCurveFitter curveFitter = GaussianCurveFitter.create();

    WeightedObservedPoints points = new WeightedObservedPoints();
    for(int i=0; i<x.length; i++) {
      points.add(x[i], y[i]);
    }

    List<WeightedObservedPoint> pointList = points.toList();

    double[] guess = new GaussianCurveFitter.ParameterGuesser(pointList).guess();
    curveFitter = curveFitter.withStartPoint(guess);

    double[] coef = curveFitter.fit(pointList);
    Gaussian gaussian = new Gaussian(coef[0], coef[1], coef[2]);
    List list = new ArrayList();
    for(double xvalue : x) {
      double yvalue= gaussian.value(xvalue);
      list.add(yvalue);
    }

    return new VectorFunction(gaussian, list);
  }
}
