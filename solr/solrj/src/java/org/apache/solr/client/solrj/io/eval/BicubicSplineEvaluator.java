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

import org.apache.commons.math3.analysis.BivariateFunction;
import org.apache.commons.math3.analysis.interpolation.PiecewiseBicubicSplineInterpolator;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class BicubicSplineEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public BicubicSplineEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  public Object doWork(Object... objects) throws IOException {

    if(objects.length != 3) {
      throw new IOException("The bicubicSpline function requires three paremeters,"+objects.length+" found.");
    }

    Object first = objects[0];
    Object second = objects[1];
    Object third = objects[2];

    double[] x = null;
    double[] y = null;
    double[][] grid = null;

    if(first instanceof List && second instanceof List && third instanceof Matrix) {
      @SuppressWarnings({"unchecked"})
      List<Number> xlist = (List<Number>) first;
      x = new double[xlist.size()];

      for(int i=0; i<x.length; i++) {
        x[i]=xlist.get(i).doubleValue();
      }

      @SuppressWarnings({"unchecked"})
      List<Number> ylist = (List<Number>) second;
      y = new double[ylist.size()];

      for(int i=0; i<y.length; i++) {
        y[i] = ylist.get(i).doubleValue();
      }

      Matrix matrix = (Matrix)third;
      grid = matrix.getData();

      PiecewiseBicubicSplineInterpolator interpolator = new PiecewiseBicubicSplineInterpolator();
      BivariateFunction bivariateFunction = interpolator.interpolate(x, y, grid);
      return bivariateFunction;
    } else {
      throw new IOException("The bicubicSpline function expects two numeric arrays and a matrix as parameters.");
    }
  }
}
