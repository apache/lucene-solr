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

import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class DbscanEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;


  public DbscanEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public Object doWork(Object... values) throws IOException {

    Matrix matrix = null;
    double e = 0;
    int minPoints = 1;
    DistanceMeasure distanceMeasure = new EuclideanDistance();

    if(values.length < 3 || values.length > 4) {
      throw new IOException("The dbscan scan function requires 3 or 4 parameters.");
    }

    if(values[0] instanceof Matrix) {
      matrix = (Matrix)values[0];
    } else {
      throw new IOException("The first parameter for dbscan should be the observation matrix.");
    }

    if(values[1] instanceof Number) {
      e = ((Number)values[1]).doubleValue();
    } else {
      throw new IOException("The second parameter for dbscan should be e.");
    }

    if(values[2] instanceof Number) {
      minPoints = ((Number)values[2]).intValue();
    } else {
      throw new IOException("The third parameter for dbscan should be minPoints.");
    }

    if(values.length > 3) {
      distanceMeasure = (DistanceMeasure)values[3];
    }

    @SuppressWarnings({"rawtypes"})
    DBSCANClusterer<ClusterPoint> dbscan = new DBSCANClusterer(e, minPoints, distanceMeasure);
    List<ClusterPoint> points = new ArrayList<>();
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

    @SuppressWarnings({"rawtypes"})
    Map fields = new HashMap();

    fields.put("e", e);
    fields.put("minPoints", minPoints);
    fields.put("distance", distanceMeasure.toString());

    return new ClusterTuple(fields, dbscan.cluster(points), matrix.getColumnLabels());
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
    private List<Cluster<ClusterPoint>> clusters;

    public ClusterTuple(@SuppressWarnings({"rawtypes"})Map fields,
                        List<Cluster<ClusterPoint>> clusters,
                        List<String> columnLabels) {
      super(fields);
      this.clusters = clusters;
      this.columnLabels = columnLabels;
    }

    public List<String> getColumnLabels() {
      return this.columnLabels;
    }

    public List<Cluster<ClusterPoint>> getClusters() {
      return this.clusters;
    }
  }
}

