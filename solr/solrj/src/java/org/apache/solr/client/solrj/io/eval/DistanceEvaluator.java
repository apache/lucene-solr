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
import java.util.Locale;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class DistanceEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public enum DistanceType {euclidean, manhattan, canberra, earthMovers}
  private DistanceType type;

  public DistanceEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public Object doWork(Object ... values) throws IOException{

    if(values.length == 1) {
      if (values[0] instanceof Matrix) {
        Matrix matrix = (Matrix) values[0];
        EuclideanDistance euclideanDistance = new EuclideanDistance();
        return distance(euclideanDistance, matrix);
      } else {
        throw new IOException("distance function operates on either two numeric arrays or a single matrix as parameters.");
      }
    } else if(values.length == 2) {
      Object first = values[0];
      Object second = values[1];

      if (null == first) {
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - null found for the first value", toExpression(constructingFactory)));
      }

      if (null == second) {
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - null found for the second value", toExpression(constructingFactory)));
      }

      if(first instanceof Matrix) {
        Matrix matrix = (Matrix) first;
        DistanceMeasure distanceMeasure = (DistanceMeasure)second;
        return distance(distanceMeasure, matrix);
      } else {
        if (!(first instanceof List<?>)) {
          throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the first value, expecting a list of numbers", toExpression(constructingFactory), first.getClass().getSimpleName()));
        }

        if (!(second instanceof List<?>)) {
          throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the second value, expecting a list of numbers", toExpression(constructingFactory), first.getClass().getSimpleName()));
        }

        DistanceMeasure distanceMeasure = new EuclideanDistance();
        return distanceMeasure.compute(
            ((List) first).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray(),
            ((List) second).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray()
        );
      }
    } else if (values.length == 3) {
      Object first = values[0];
      Object second = values[1];
      DistanceMeasure distanceMeasure = (DistanceMeasure)values[2];

      if (null == first) {
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - null found for the first value", toExpression(constructingFactory)));
      }

      if (null == second) {
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - null found for the second value", toExpression(constructingFactory)));
      }

      if (!(first instanceof List<?>)) {
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the first value, expecting a list of numbers", toExpression(constructingFactory), first.getClass().getSimpleName()));
      }

      if (!(second instanceof List<?>)) {
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the second value, expecting a list of numbers", toExpression(constructingFactory), first.getClass().getSimpleName()));
      }

      return distanceMeasure.compute(
          ((List) first).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray(),
          ((List) second).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray()
      );
    } else {
      throw new IOException("distance function operates on either two numeric arrays or a single matrix as parameters.");
    }
  }

  private Matrix distance(DistanceMeasure distanceMeasure, Matrix matrix) {
    double[][] data = matrix.getData();
    Array2DRowRealMatrix realMatrix = new Array2DRowRealMatrix(data, false);
    realMatrix = (Array2DRowRealMatrix)realMatrix.transpose();
    data = realMatrix.getDataRef();
    double[][] distanceMatrix = new double[data.length][data.length];
    for(int i=0; i<data.length; i++) {
      double[] row = data[i];
      for(int j=0; j<data.length; j++) {
        double[] row2 = data[j];
        double dist = distanceMeasure.compute(row, row2);
        distanceMatrix[i][j] = dist;
      }
    }
    Matrix m = new Matrix(distanceMatrix);
    List<String> labels = CorrelationEvaluator.getColumnLabels(matrix.getColumnLabels(), data.length);
    m.setColumnLabels(labels);
    m.setRowLabels(labels);
    return m;
  }
}