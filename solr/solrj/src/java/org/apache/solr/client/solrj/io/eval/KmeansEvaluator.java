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
import java.util.Map;
import java.util.HashMap;


import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class KmeansEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;



  public KmeansEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  public Object doWork(Object... values) throws IOException {

    if(values.length < 2) {
      throw new IOException("kmeans expects atleast two parameters a Matrix of observations and k");
    }

    Matrix matrix = null;
    int k = 0;
    int maxIterations = 1000;

    if(values[0] instanceof Matrix) {
      matrix = (Matrix)values[0];
    } else {
      throw new IOException("The first parameter for kmeans should be the observation matrix.");
    }

    if(values[1] instanceof Number) {
      k = ((Number)values[1]).intValue();
    } else {
      throw new IOException("The second parameter for kmeans should be k.");
    }

    if(values.length == 3) {
      maxIterations = ((Number)values[2]).intValue();
    }

    KMeansPlusPlusClusterer<ClusterPoint> kmeans = new KMeansPlusPlusClusterer(k, maxIterations);
    List<ClusterPoint> points = new ArrayList();
    double[][] data = matrix.getData();

    List<String> ids = matrix.getRowLabels();

    for(int i=0; i<data.length; i++) {
      double[] vec = data[i];
      points.add(new ClusterPoint(ids.get(i), vec));
    }

    Map fields = new HashMap();

    fields.put("k", k);
    fields.put("distance", "euclidean");
    fields.put("maxIterations", maxIterations);

    return new ClusterTuple(fields, kmeans.cluster(points), matrix.getColumnLabels());
  }

  public static class ClusterPoint implements Clusterable {

    private double[] point;
    private String id;

    public ClusterPoint(String id, double[] point) {
      this.id = id;
      this.point = point;
    }

    public double[] getPoint() {
      return this.point;
    }

    public String getId() {
      return this.id;
    }
  }

  public static class ClusterTuple extends Tuple {

    private List<String> columnLabels;
    private List<CentroidCluster<ClusterPoint>> clusters;

    public ClusterTuple(Map fields,
                        List<CentroidCluster<ClusterPoint>> clusters,
                        List<String> columnLabels) {
      super(fields);
      this.clusters = clusters;
      this.columnLabels = columnLabels;
    }

    public List<String> getColumnLabels() {
      return this.columnLabels;
    }

    public List<CentroidCluster<ClusterPoint>> getClusters() {
      return this.clusters;
    }




  }
}

