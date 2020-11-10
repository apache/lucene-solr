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
import java.util.TreeSet;

import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class KnnEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public KnnEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  public Object doWork(Object... values) throws IOException {

    if(values.length < 3) {
      throw new IOException("knn expects three parameters a Matrix, numeric array and k");
    }

    Matrix matrix = null;
    double[] vec = null;
    int k = 0;

    if(values[0] instanceof Matrix) {
      matrix = (Matrix)values[0];
    } else {
      throw new IOException("The first parameter for knn should be a matrix.");
    }

    if(values[1] instanceof List) {
      @SuppressWarnings({"unchecked"})
      List<Number> nums = (List<Number>)values[1];
      vec = new double[nums.size()];
      for(int i=0; i<nums.size(); i++) {
        vec[i] = nums.get(i).doubleValue();
      }
    } else {
      throw new IOException("The second parameter for knn should be a numeric array.");
    }

    if(values[2] instanceof Number) {
      k = ((Number)values[2]).intValue();
    } else {
      throw new IOException("The third parameter for knn should be k.");
    }

    DistanceMeasure distanceMeasure = null;

    if(values.length == 4) {
      distanceMeasure = (DistanceMeasure)values[3];
    } else {
      distanceMeasure = new EuclideanDistance();
    }

    return search(matrix, vec, k, distanceMeasure);
  }

  public static Matrix search(Matrix observations,
                              double[] vec,
                              int k,
                              DistanceMeasure distanceMeasure) {

    double[][] data = observations.getData();
    TreeSet<Neighbor> neighbors = new TreeSet<>();
    for(int i=0; i<data.length; i++) {
      double distance = distanceMeasure.compute(vec, data[i]);
      neighbors.add(new Neighbor(i, distance));
      if(neighbors.size() > k) {
        neighbors.pollLast();
      }
    }

    double[][] out = new double[neighbors.size()][];
    List<String> rowLabels = observations.getRowLabels();
    List<String> newRowLabels = new ArrayList<>();
    List<Number> indexes = new ArrayList<>();
    List<Number> distances = new ArrayList<>();
    int i=-1;

    while(neighbors.size() > 0) {
      Neighbor neighbor = neighbors.pollFirst();
      int rowIndex = neighbor.getRow();

      if(rowLabels != null) {
        newRowLabels.add(rowLabels.get(rowIndex));
      }

      out[++i] = data[rowIndex];
      distances.add(neighbor.getDistance());
      indexes.add(rowIndex);
    }

    Matrix knn = new Matrix(out);

    if(rowLabels != null) {
      knn.setRowLabels(newRowLabels);
    }

    knn.setColumnLabels(observations.getColumnLabels());
    knn.setAttribute("distances", distances);
    knn.setAttribute("indexes", indexes);
    return knn;
  }

  public static class Neighbor implements Comparable<Neighbor> {

    private Double distance;
    private int row;

    public Neighbor(int row, double distance) {
      this.distance = distance;
      this.row = row;
    }

    public int getRow() {
      return this.row;
    }

    public Double getDistance() {
      return distance;
    }

    public int compareTo(Neighbor neighbor) {
      if(this.distance.compareTo(neighbor.getDistance()) == 0) {
        return row-neighbor.getRow();
      }

      return this.distance.compareTo(neighbor.getDistance());
    }
  }
}

