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
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class KmeansEvaluator extends RecursiveObjectEvaluator implements TwoValueWorker {
  protected static final long serialVersionUID = 1L;

  private int maxIterations = 1000;

  public KmeansEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);

    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(namedParam.getName().equals("maxIterations")) {
        this.maxIterations = Integer.parseInt(namedParam.getParameter().toString().trim());
      } else {
        throw new IOException("Unexpected named parameter:"+namedParam.getName());
      }
    }
  }

  @Override
  public Object doWork(Object value1, Object value2) throws IOException {

    Matrix matrix = null;
    int k = 0;

    if(value1 instanceof Matrix) {
      matrix = (Matrix)value1;
    } else {
      throw new IOException("The first parameter for kmeans should be the observation matrix.");
    }

    if(value2 instanceof Number) {
      k = ((Number)value2).intValue();
    } else {
      throw new IOException("The second parameter for kmeans should be k.");
    }


    KMeansPlusPlusClusterer<ClusterPoint> kmeans = new KMeansPlusPlusClusterer(k, maxIterations);
    List<ClusterPoint> points = new ArrayList();
    double[][] data = matrix.getData();

    List<String> ids = matrix.getRowLabels();

    for(int i=0; i<data.length; i++) {
      double[] vec = data[i];
      if(ids != null) {
        points.add(new ClusterPoint(ids.get(i), vec));
      } else {
        points.add(new ClusterPoint(Integer.toString(i), vec));
      }
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
    private Matrix membershipMatrix;

    public ClusterTuple(Map fields,
                        List<CentroidCluster<ClusterPoint>> clusters,
                        List<String> columnLabels) {
      super(fields);
      this.clusters = clusters;
      this.columnLabels = columnLabels;
    }

    public ClusterTuple(Map fields,
                        List<CentroidCluster<ClusterPoint>> clusters,
                        List<String> columnLabels,
                        Matrix membershipMatrix) {
      super(fields);
      this.clusters = clusters;
      this.columnLabels = columnLabels;
      this.membershipMatrix = membershipMatrix;
    }

    public Matrix getMembershipMatrix() {
      return this.membershipMatrix;
    }

    public List<String> getColumnLabels() {
      return this.columnLabels;
    }

    public List<CentroidCluster<ClusterPoint>> getClusters() {
      return this.clusters;
    }
  }
}

